import logging

from ckanext.mysql2mongodb.dataconv.constant.consts import SQL_FILE_EXTENSION
from ckanext.mysql2mongodb.dataconv.constant.error_codes import TASK_PREPARE_DATA_ERROR, INPUT_FILE_EXTENSION_ERROR, \
    TASK_CONVERT_SCHEMA_ERROR
from ckanext.mysql2mongodb.dataconv.database.mongo_handler import MongoHandler
from ckanext.mysql2mongodb.dataconv.database.mysql_handler import MySQLHandler
from ckanext.mysql2mongodb.dataconv.exceptions import InvalidFileExtensionError
from ckanext.mysql2mongodb.dataconv.util import command_lines

logger = logging.getLogger(__name__)


def prepare(sql_file_url: str, resource_id: str, sql_file_name: str):
    try:
        # check sql file type
        if sql_file_name.split('.')[-1] != SQL_FILE_EXTENSION:
            logger.error(f'error code: {INPUT_FILE_EXTENSION_ERROR}')
            raise InvalidFileExtensionError('Invalid MySQL backup file extension!')
        # region Main tasks
        command_lines.download_from_ckan_mysql_file(sql_file_url, resource_id, sql_file_name)
        # endregion
    except Exception as ex:
        logger.error(f'error code: {TASK_PREPARE_DATA_ERROR}')
        raise ex


def convert_schema(resource_id: str, sql_file_name: str):
    try:
        # region Init database handlers
        mysql_handler = MySQLHandler()
        mongo_handler = MongoHandler()
        # endregion
        # region Main tasks
        mysql_handler.restore_from_ckan(resource_id, sql_file_name)
        mysql_handler.generate_schema(resource_id, sql_file_name)
        mongo_handler.import_mysql_schema_json(resource_id, sql_file_name)
        # endregion
    except Exception as ex:
        logger.error(f'error code: {TASK_CONVERT_SCHEMA_ERROR}')
        raise ex


def convert_data(resource_id: str, sql_file_name: str):
    """
    Steps:
    - Migrate mysql data to mongo
    - Convert relations to references
    """
    # region Init database handlers
    mysql_handler = MySQLHandler()
    mongo_handler = MongoHandler()
    db_name = sql_file_name.split('.')[0]
    # endregion
    # region Main tasks
    migrate_data = []
    table_schema_datatypes = mongo_handler.get_table_column_and_data_type(db_name)
    for table_name in mongo_handler.get_schema_table_name_list(db_name):
        try:
            fetched_data = mysql_handler.fetch_data_for_mongo(db_name, table_name, table_schema_datatypes[table_name])
            converted_data = mongo_handler.store_fetched_mysql_data(db_name, table_name, fetched_data)
            element = (table_name, converted_data)
            migrate_data.append(element)
            # Store migrate_data to mongodb
            mongo_handler.convert_relations_to_references()
        except KeyError:
            continue
    # endregion


def upload_converted_data():
    logger.info('=========================== Load Result ===========================')
