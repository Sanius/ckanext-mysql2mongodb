import logging

from ckanext.mysql2mongodb.dataconv.util.data_conversion import convert_mysql_to_mongodb

from ckanext.mysql2mongodb.dataconv.constant.consts import SQL_FILE_EXTENSION
from ckanext.mysql2mongodb.dataconv.constant.error_codes import TASK_PREPARE_DATA_ERROR, INPUT_FILE_EXTENSION_ERROR, \
    TASK_CONVERT_SCHEMA_ERROR, TASK_CONVERT_DATA_ERROR, TASK_DUMP_DATA_ERROR, TASK_UPLOAD_DATA_ERROR
from ckanext.mysql2mongodb.dataconv.database.mongo_handler import MongoHandler
from ckanext.mysql2mongodb.dataconv.database.mysql_handler import MySQLHandler
from ckanext.mysql2mongodb.dataconv.exceptions import InvalidFileExtensionError
from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler

logger = logging.getLogger(__name__)


def prepare(sql_file_url: str, resource_id: str, sql_file_name: str):
    try:
        # check sql file type
        if sql_file_name.split('.')[-1] != SQL_FILE_EXTENSION:
            logger.error(f'error code: {INPUT_FILE_EXTENSION_ERROR}')
            raise InvalidFileExtensionError('Invalid MySQL backup file extension!')
        # region Main tasks
        file_system_handler.download_from_ckan_mysql_file(sql_file_url, resource_id, sql_file_name)
        # endregion
        logger.info('Task prepare success')
    except Exception as ex:
        logger.error(f'error code: {TASK_PREPARE_DATA_ERROR}')
        raise ex


def convert_schema(resource_id: str, sql_file_name: str):
    try:
        # region Init database handlers
        mysql_handler = MySQLHandler()
        mongo_handler = MongoHandler()

        mongo_handler.drop_old_db(sql_file_name.split('.')[0])
        # endregion
        # region Main tasks
        mysql_handler.restore_from_ckan(resource_id, sql_file_name)
        mysql_handler.generate_schema_file(resource_id, sql_file_name)
        mongo_handler.import_mysql_schema_json(resource_id, sql_file_name)
        # endregion
        logger.info('Task convert schema success')
    except Exception as ex:
        logger.error(f'error code: {TASK_CONVERT_SCHEMA_ERROR}')
        raise ex


def convert_data(sql_file_name: str):
    """
    Steps:
    - Migrate mysql data to mongo
    - Convert relations to references
    """
    try:
        # region Init database handlers
        db_name = sql_file_name.split('.')[0]
        mysql_handler = MySQLHandler()
        mongo_handler = MongoHandler()
        # endregion
        # region Main tasks
        column_type_map = mongo_handler.get_table_columnname_datatype(db_name)
        for table_name in mongo_handler.get_schema_table_name_list(db_name):
            data_generator = mysql_handler.fetch_data_for_mongo(db_name, table_name, column_type_map[table_name])
            for fetched_data in data_generator:
                converted_data = convert_mysql_to_mongodb(fetched_data, column_type_map[table_name])
                mongo_handler.store_data_to_collection(db_name, table_name, converted_data)
        logger.info('Task convert data success')
    except Exception as ex:
        logger.error(f'error code: {TASK_CONVERT_DATA_ERROR}')
        raise ex
    # endregion


def dump_data(resource_id: str, sql_file_name: str):
    try:
        mongo_handler = MongoHandler()
        mongo_handler.dump_database(resource_id, sql_file_name)
        logger.info('Task dump data success')
    except Exception as ex:
        logger.error(f'error code: {TASK_DUMP_DATA_ERROR}')
        raise ex


def upload_converted_data(resource_id: str, sql_file_name: str, package_id: str):
    try:
        file_system_handler.upload_to_ckan_mongo_dump_data(resource_id, sql_file_name, package_id)
        logger.info('Task upload data success')
    except Exception as ex:
        logger.error(f'error code: {TASK_UPLOAD_DATA_ERROR}')
        raise ex
