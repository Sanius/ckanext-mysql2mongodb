import logging
import pprint

from ckanext.mysql2mongodb.dataconv.exceptions import InvalidFileExtensionError

from ckanext.mysql2mongodb.dataconv.util import command_lines

from ckanext.mysql2mongodb.dataconv.constant.error_codes import TASK_PREPARE_DATA_ERROR

from ckanext.mysql2mongodb.dataconv.constant.consts import SQL_FILE_EXTENSION

logger = logging.getLogger(__name__)


def prepare(sql_file_url: str, resource_id: str, sql_file_name: str):
    try:
        # check sql file type
        if sql_file_name.split('.')[-1] != SQL_FILE_EXTENSION:
            logger.error(f'error code: {TASK_PREPARE_DATA_ERROR}')
            raise InvalidFileExtensionError('Invalid MySQL backup file extension!')

        # change dir
        # os.system("whoami")
        # LOCATION = os.getenv('WORKSPACE_LOCATION')
        # LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
        # os.chdir(LOCATION)

        # Read configurations
        # db_conf = read_database_config()
        # package_conf = read_package_config()
        # CKAN_API_KEY = package_conf["X-CKAN-API-Key"]

        # get sql bak
        # os.system(f"mkdir -p ./downloads/{resource_id}
        # os.system(
        #     f"curl -H \"X-CKAN-API-Key: {CKAN_API_KEY}\" -o ./downloads/{resource_id}/{sql_file_name} {sql_file_url}")
        command_lines.download_from_ckan_mysql_file(sql_file_url, resource_id, sql_file_name)

        # get mysql info
        # schema_name = sql_file_name.split(".")[0]
        # mysql_host = db_conf["mysql_host"]
        # mysql_username = db_conf["mysql_username"]
        # mysql_password = db_conf["mysql_password"]
        # mysql_port = db_conf["mysql_port"]
        # mysql_dbname = schema_name

        # process mysql
        # mysql_conn = open_connection_mysql(
        #     mysql_host, mysql_username, mysql_password)
        # mysql_cur = mysql_conn.cursor()
        # mysql_cur.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_dbname};")
        # mysql_cur.close()
        # mysql_conn.close()

        os.system(
            f"mysql -h {mysql_host} -u {mysql_username} --password={mysql_password} {schema_name} < {LOCATION}/downloads/{resource_id}/{sql_file_name}")

        # push_to_xcom(kwargs, resource_id, sql_file_name, sql_file_url, db_conf, package_conf, CKAN_API_KEY,
        #              schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname)

    except Exception as ex:
        logger.error(pprint.pprint(ex))
        raise ex


def converse_schema():
    logger.info('=========================== Converse Schema ===========================')


def converse_data():
    logger.info('=========================== Converse Schema ===========================')


def upload_converted_data():
    logger.info('=========================== Load Result ===========================')
