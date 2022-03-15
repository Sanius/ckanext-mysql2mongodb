import logging
import subprocess
from typing import Any

from ckanext.mysql2mongodb.dataconv.constant.consts import MYSQL, SCHEMA_CRAWLER

from ckanext.mysql2mongodb.dataconv.util import command_lines

from ckanext.mysql2mongodb.dataconv.exceptions import UnspecifiedDatabaseException

from ckanext.mysql2mongodb.dataconv.constant.error_codes import MYSQL_DATABASE_CONNECTION_ERROR, \
    MYSQL_UNSPECIFIED_DATABASE_ERROR, MYSQL_EXPORT_SCHEMA_ERROR
from mysql import connector as mysql_connector
from ckanext.mysql2mongodb.dataconv.database.abstract_database_handler import AbstractDatabaseHandler

from ckanext.mysql2mongodb.dataconv.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USERNAME, MYSQL_PASSWORD, \
    MYSQL_ENV_VAR_PATH, SCHEMA_CRAWLER_ENV_VAR_PATH

logger = logging.getLogger(__name__)


class MySQLHandler(AbstractDatabaseHandler):
    _SCHEMA_FILE_NAME = 'schema.json'

    # Override
    def __init__(self):
        super().__init__()
        self._host = MYSQL_HOST
        self._port = MYSQL_PORT
        self._username = MYSQL_USERNAME
        self._password = MYSQL_PASSWORD

    # Override
    def restore_from_ckan(self, resource_id: str, file_name: str):
        """
        e.g. file_name: sakila.sql
        """
        # Get db name
        name = file_name.split('.')[0]
        self.set_db(name)
        self.create_db()
        # Get file path
        file_path = f'{command_lines.get_ckan_download_cache_path(resource_id)}/{file_name}'
        mysql_command = MYSQL_ENV_VAR_PATH + MYSQL
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

    # Override
    def backup(self):
        pass

    # Override
    def set_db(self, db: str):
        self._db = db

    def create_db(self):
        if not self._db:
            logger.error(f'error code: {MYSQL_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        conn = self._get_open_connection()
        db_cursor = conn.cursor()
        try:
            db_cursor.execute(f'CREATE DATABASE IF NOT EXISTS {self._db};')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_UNSPECIFIED_DATABASE_ERROR}')
            raise ex
        finally:
            db_cursor.close()
            conn.close()

    def get_db_connection(self) -> Any:
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
            return conn
        except Exception as ex:
            logger.error(f'error code: {MYSQL_DATABASE_CONNECTION_ERROR}')
            raise ex

    def generate_schema(self):
        """
        Generate MySQL schema using SchemaCrawler, then save as JSON file at intermediate directory.
        """
        if not self._db:
            logger.error(f'error code: {MYSQL_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        try:
            schema_crawler_cache_dir = command_lines.create_schema_crawler_cache_dir(self._db)
            schema_crawler_command = SCHEMA_CRAWLER_ENV_VAR_PATH + SCHEMA_CRAWLER
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
                schema_crawler_file_path=f'{schema_crawler_cache_dir}/{self._SCHEMA_FILE_NAME}'
            )
            subprocess.run([command_line_str], check=True, shell=True)
            logger.info(f'Generate MySQL database {self._db} schema successfully!')
        except Exception as ex:
            logger.error(f'error code: {MYSQL_EXPORT_SCHEMA_ERROR}')
            raise ex

    def _get_open_connection(self) -> Any:
        try:
            conn = mysql_connector.connect(
                host=self._host,
                port=self._port,
                user=self._username,
                password=self._password,
                auth_plugin='mysql_native_password'
            )
            return conn
        except Exception as ex:
            logger.error(f'error code: {MYSQL_DATABASE_CONNECTION_ERROR}')
            raise ex
