import logging
import subprocess
from typing import Generator, List
import pandas as pd

from sqlalchemy import MetaData, create_engine, select, inspect
from sqlalchemy.engine import Connection as SQLalchemyConnection
from sqlalchemy.testing.schema import Table

from ckanext.mysql2mongodb.dataconv.constant.consts import MYSQL, SCHEMA_CRAWLER, JSON_FILE_EXTENSION, MYSQL_MONGO_MAP, \
    MONGO_SINGLE_GEOMETRY_DATATYPE, DATABASE_CHUNK_SIZE

from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler

from ckanext.mysql2mongodb.dataconv.exceptions import DatabaseConnectionError, \
    DatatypeMappingException, MySQLDatabaseNotFoundException, UnspecifiedDatabaseException, MySQLTableNotFoundError

from ckanext.mysql2mongodb.dataconv.constant.error_codes import MYSQL_DATABASE_CONNECTION_ERROR, \
    MYSQL_EXPORT_SCHEMA_ERROR, MYSQL_RESTORE_DATA_ERROR, \
    MYSQL_FETCH_DATA_TO_MONGO_ERROR, MYSQL_CREATE_DATABASE_ERROR, MYSQL_DATABASE_NOT_FOUND_ERROR, \
    MYSQL_UNSPECIFIED_DATABASE_ERROR, MYSQL_TABLE_NOT_FOUND_ERROR, MYSQL_UNABLE_TO_CREATE_PANDAS_DATAFRAME_ERROR
from mysql import connector as mysql_connector
from ckanext.mysql2mongodb.dataconv.database.abstract_database_handler import AbstractDatabaseHandler

from ckanext.mysql2mongodb.dataconv.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USERNAME, MYSQL_PASSWORD, \
    MYSQL_ENV_VAR_PATH, SCHEMA_CRAWLER_ENV_VAR_PATH

logger = logging.getLogger(__name__)


class MySQLHandler(AbstractDatabaseHandler):
    # Override
    def __init__(self):
        super().__init__()
        self._host = MYSQL_HOST
        self._port = MYSQL_PORT
        self._username = MYSQL_USERNAME
        self._password = MYSQL_PASSWORD
        self._metadata = MetaData()

    # region Task methods
    def restore_from_ckan(self, resource_id: str, file_name: str):
        """
        e.g. file_name: sakila.sql
        """
        db_name = file_name.split('.')[0]
        try:
            self._drop_db_if_exists(db_name)
            self._create_db(db_name)
            # Get file path
            file_path = f'{file_system_handler.get_ckan_download_cache_path(resource_id)}/{file_name}'
            self._restore(db_name, file_path)
            logger.info(f'Restore MySQL database successfully')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_RESTORE_DATA_ERROR}')
            raise ex

    def generate_schema_file(self, resource_id: str, file_name: str):
        """
        Generate MySQL schema using SchemaCrawler, then save as JSON file at intermediate directory.
        """
        db_name = file_name.split('.')[0]
        try:
            if not db_name or not self._does_db_exist(db_name):
                logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
                raise MySQLDatabaseNotFoundException('Database not found')

            schema_crawler_cache_dir = file_system_handler.create_schema_crawler_cache_dir(resource_id)
            file_path = f'{schema_crawler_cache_dir}/{db_name}.{JSON_FILE_EXTENSION}'
            self._generate_schema_file(db_name, file_path)
            logger.info(f'Generate MySQL database {db_name} schema successfully!')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_EXPORT_SCHEMA_ERROR}')
            raise ex

    def fetch_data_for_mongo(self, db_name: str, table_name: str, column_datatype_list: List) -> Generator:
        """
        column_datatype_map = { <column_name>: <column_datatype> }
        """
        conn = self._get_db_connection(db_name)
        try:
            sql_cmd = 'SELECT'
            for column in column_datatype_list:
                column_name = column['column_name']
                mysql_datatype = column['column_datatype']
                mongo_datatype = MYSQL_MONGO_MAP.get(mysql_datatype)
                if not mongo_datatype:
                    raise DatatypeMappingException(f'Data type {mysql_datatype} has not been handled!')
                elif mongo_datatype == MONGO_SINGLE_GEOMETRY_DATATYPE:
                    sql_cmd = f'{sql_cmd} ST_AsText({column_name}),'
                else:
                    sql_cmd = f'{sql_cmd} `{column_name}`,'
            sql_cmd = f'{sql_cmd[:-1]} FROM `{table_name}`'

            with conn.cursor() as cursor:
                cursor.execute(sql_cmd)
                while True:
                    if not (rows := cursor.fetchmany(DATABASE_CHUNK_SIZE)):
                        break
                    yield rows
                conn.commit()

            logger.info(f'Fetch data from database: {db_name} table: {table_name} successfully!')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_FETCH_DATA_TO_MONGO_ERROR}')
            raise ex
        finally:
            conn.close()

    def to_pandas_dataframe(self, db_name: str, table_name: str, index_cols: List) -> pd.DataFrame:
        try:
            db_conn = self._get_db_connection_by_engine(db_name)
            if not self._does_table_exists(db_name, table_name):
                logger.error(f'error code: {MYSQL_TABLE_NOT_FOUND_ERROR}')
                raise MySQLTableNotFoundError(f'Mysql table {table_name} not found')
            target_table = Table(table_name, self._metadata, autoload_with=db_conn)
            if not index_cols:
                index_cols = None
            return pd.read_sql(select([target_table]), con=db_conn, index_col=index_cols)
        except Exception as ex:
            logger.error(f'error code: {MYSQL_UNABLE_TO_CREATE_PANDAS_DATAFRAME_ERROR}')
            raise ex
    # endregion

    # region Component methods
    def _create_db(self, db_name: str):
        conn = self._get_open_connection()
        try:
            if not db_name:
                logger.error(f'error code: {MYSQL_UNSPECIFIED_DATABASE_ERROR}')
                raise UnspecifiedDatabaseException('Database name is incorrect')
            with conn.cursor() as db_cursor:
                db_cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name};')
                conn.commit()
        except Exception as ex:
            logger.error(f'error code: {MYSQL_CREATE_DATABASE_ERROR}')
            raise ex
        finally:
            conn.close()

    def _restore(self, db_name: str, file_path: str):
        if not db_name or not self._does_db_exist(db_name):
            logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
            raise MySQLDatabaseNotFoundException('Database not found')

        mysql_command = f'{MYSQL_ENV_VAR_PATH}/{MYSQL}'
        command_line_str = '''
            {command} -h {mysql_host} \
            -P {mysql_port} \
            --user={mysql_user} \
            --password={mysql_password} {mysql_database} < {file_path} \
        '''.format(command=mysql_command,
                   mysql_host=self._host,
                   mysql_port=self._port,
                   mysql_user=self._username,
                   mysql_password=self._password,
                   mysql_database=db_name,
                   file_path=file_path)
        subprocess.run([command_line_str], check=True, shell=True)

    def _generate_schema_file(self, db_name: str, file_path: str):
        if not db_name or not self._does_db_exist(db_name):
            logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
            raise MySQLDatabaseNotFoundException('Database not found')

        schema_crawler_command = f'{SCHEMA_CRAWLER_ENV_VAR_PATH}/{SCHEMA_CRAWLER}'
        command_line_str = '''
            {command} --server=mysql \
            --host={mysql_host} \
            --port={mysql_port} \
            --database={mysql_database} \
            --schemas={mysql_database} \
            --user={mysql_user} \
            --password={mysql_password} \
            --info-level=maximum \
            --command=serialize \
            --output-file={schema_crawler_file_path} \
        '''.format(
            command=schema_crawler_command,
            mysql_host=self._host,
            mysql_port=self._port,
            mysql_database=db_name,
            mysql_user=self._username,
            mysql_password=self._password,
            schema_crawler_file_path=file_path
        )
        subprocess.run([command_line_str], check=True, shell=True)

    def _does_table_exists(self, db_name: str, table_name: str) -> bool:
        db_conn = self._get_db_connection_by_engine(db_name)
        return False if not table_name else inspect(db_conn).has_table(table_name)

    def _drop_db_if_exists(self, db_name: str):
        conn = self._get_open_connection()
        try:
            with conn.cursor() as db_cursor:
                db_cursor.execute(f'DROP DATABASE IF EXISTS {db_name};')
                conn.commit()
        except Exception as ex:
            logger.error(f'error code: {MYSQL_CREATE_DATABASE_ERROR}')
            raise ex
        finally:
            conn.close()

    def _get_db_connection_by_engine(self, db_name: str) -> SQLalchemyConnection:
        try:
            if not db_name or not self._does_db_exist(db_name):
                logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
                raise MySQLDatabaseNotFoundException('Database not found')

            connection_info = 'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}' \
                .format(
                    mysql_host=self._host,
                    mysql_port=self._port,
                    mysql_user=self._username,
                    mysql_password=self._password,
                    mysql_db=db_name
                )

            engine = create_engine(connection_info)
            return engine.connect()
        except Exception as ex:
            logger.error(f'error code: {MYSQL_DATABASE_CONNECTION_ERROR}')
            raise ex

    def _get_db_connection(self, db_name: str) -> mysql_connector.CMySQLConnection:
        try:
            if not db_name or not self._does_db_exist(db_name):
                logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
                raise MySQLDatabaseNotFoundException('Database not found')

            conn = mysql_connector.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password,
                database=db_name
            )
            if not conn.is_connected():
                raise DatabaseConnectionError(f'Unable to connect to mysql database {db_name}')
            return conn
        except Exception as ex:
            logger.error(f'error code: {MYSQL_DATABASE_CONNECTION_ERROR}')
            raise ex
    # endregion

    # region Inheritance methods
    # Override
    def _does_db_exist(self, db_name: str) -> bool:
        exists = False
        if db_name:
            conn = self._get_open_connection()
            with conn.cursor() as cur:
                cur.execute('SHOW DATABASES;')
                exists = db_name in set(db[0] for db in cur.fetchall())
            conn.close()
        return exists

    # Override
    def _get_open_connection(self) -> mysql_connector.CMySQLConnection:
        try:
            conn = mysql_connector.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password,
                auth_plugin='mysql_native_password'
            )
            if not conn.is_connected():
                raise DatabaseConnectionError(f'Unable to connect to mysql')
            return conn
        except Exception as ex:
            logger.error(f'error code: {MYSQL_DATABASE_CONNECTION_ERROR}')
            raise ex
    # endregion
