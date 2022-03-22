import logging
import subprocess
from typing import Any, List

from ckanext.mysql2mongodb.dataconv.constant.consts import MYSQL, SCHEMA_CRAWLER, JSON_FILE_EXTENSION, MYSQL_MONGO_MAP

from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler

from ckanext.mysql2mongodb.dataconv.exceptions import UnspecifiedDatabaseException, DatabaseConnectionError, \
    DatatypeMappingException, MySQLDatabaseNotFoundException

from ckanext.mysql2mongodb.dataconv.constant.error_codes import MYSQL_DATABASE_CONNECTION_ERROR, \
    MYSQL_UNSPECIFIED_DATABASE_ERROR, MYSQL_EXPORT_SCHEMA_ERROR, MYSQL_RESTORE_DATA_ERROR, \
    MYSQL_FETCH_DATA_TO_MONGO_ERROR, MYSQL_CREATE_DATABASE_ERROR, MYSQL_DATABASE_NOT_FOUND_ERROR
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

    # region Task methods
    def restore_from_ckan(self, resource_id: str, file_name: str):
        """
        e.g. file_name: sakila.sql
        """
        try:
            name = file_name.split('.')[0]
            self.set_db(name)
            self._create_db()
            # Get file path
            file_path = f'{file_system_handler.get_ckan_download_cache_path(resource_id)}/{file_name}'
            self._restore(file_path)
            logger.info(f'Restore MySQL database successfully')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_RESTORE_DATA_ERROR}')
            raise ex

    def generate_schema_file(self, resource_id: str, file_name: str):
        """
        Generate MySQL schema using SchemaCrawler, then save as JSON file at intermediate directory.
        """
        db_name = file_name.split('.')[0]
        if not self._does_db_exist(db_name):
            logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
            raise MySQLDatabaseNotFoundException('Database not found')
        self.set_db(db_name)
        try:
            schema_crawler_cache_dir = file_system_handler.create_schema_crawler_cache_dir(resource_id)
            file_path = f'{schema_crawler_cache_dir}/{db_name}.{JSON_FILE_EXTENSION}'
            self._generate_schema_file(file_path)
            logger.info(f'Generate MySQL database {db_name} schema successfully!')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_EXPORT_SCHEMA_ERROR}')
            raise ex

    def fetch_data_for_mongo(self, db_name: str, table_name: str, column_type_map: dict) -> List:
        """
        column_type_dict = { <column_name>: <column_datatype> }
        """
        self.set_db(db_name)
        if not self._does_db_exist(db_name):
            logger.error(f'error code: {MYSQL_DATABASE_NOT_FOUND_ERROR}')
            raise MySQLDatabaseNotFoundException('Database not found')
        conn = self._get_db_connection()
        try:
            sql_cmd = 'SELECT'
            for column_name in column_type_map.keys():
                # col_fetch_seq.append(col_name)
                mysql_datatype = column_type_map.get(column_name)
                mongo_datatype = MYSQL_MONGO_MAP.get(mysql_datatype, '')
                # Generating SQL for selecting from MySQL Database
                if mongo_datatype is None:
                    raise DatatypeMappingException(f'Data type {mysql_datatype} has not been handled!')
                elif mongo_datatype == 'single-geometry':
                    sql_cmd = f'{sql_cmd} ST_AsText({column_name}),'
                else:
                    sql_cmd = f'{sql_cmd} `{column_name}`,'
            sql_cmd = f'{sql_cmd[:-1]} FROM `{table_name}`'
            cursor = conn.cursor()
            cursor.execute(sql_cmd)
            result = cursor.fetchall()
            conn.commit()
            cursor.close()
            logger.info(f'Fetch data from database: {db_name} table: {table_name} successfully!')
            return result
        except Exception as ex:
            logger.error(f'error code: {MYSQL_FETCH_DATA_TO_MONGO_ERROR}')
            raise ex
        finally:
            conn.cursor()
    # endregion

    # region Middleware methods
    def _create_db(self):
        if not self._db:
            logger.error(f'error code: {MYSQL_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        conn = self._get_open_connection()
        db_cursor = conn.cursor()
        try:
            db_cursor.execute(f'CREATE DATABASE IF NOT EXISTS {self._db};')
            conn.commit()
        except Exception as ex:
            logger.error(f'error code: {MYSQL_CREATE_DATABASE_ERROR}')
            raise ex
        finally:
            db_cursor.close()
            conn.close()

    def _does_db_exist(self, db_name: str) -> bool:
        exists = False
        if db_name:
            conn = self._get_db_connection()
            cur = conn.cursor()
            cur.execute('SHOW DATABASES;')
            db_list = cur.fetchall()
            for db in db_list:
                if db[0] == db_name:
                    exists = True
                    break
            cur.close()
            conn.close()
        return exists
    # endregion

    # region Component methods
    def _restore(self, file_path: str):
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
                   mysql_database=self._db,
                   file_path=file_path)
        subprocess.run([command_line_str], check=True, shell=True)

    def _generate_schema_file(self, file_path: str):
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
            mysql_database=self._db,
            mysql_user=self._username,
            mysql_password=self._password,
            schema_crawler_file_path=file_path
        )
        subprocess.run([command_line_str], check=True, shell=True)

    # Override
    def set_db(self, db: str):
        self._db = db

    def _get_db_connection(self) -> Any:
        if not self._db:
            logger.error(f'error code: {MYSQL_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        try:
            conn = mysql_connector.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password,
                database=self._db
            )
            if not conn.is_connected():
                raise DatabaseConnectionError(f'Unable to connect to mysql database {self._db}')
            return conn
        except Exception as ex:
            logger.error(f'error code: {MYSQL_DATABASE_CONNECTION_ERROR}')
            raise ex

    # Override
    def _get_open_connection(self) -> Any:
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
