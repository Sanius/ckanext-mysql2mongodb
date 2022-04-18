import logging

import numpy as np

from ckanext.mysql2mongodb.dataconv.engine import lightweight_coreset
from ckanext.mysql2mongodb.dataconv.transform import convert_mysql_to_mongodb, \
    transform_mysql_data_for_coreset_algorithm
from ckanext.mysql2mongodb.dataconv.database.validator_log_handler import ValidatorLogHandler
from ckanext.mysql2mongodb.dataconv.util.helper import from_pandas_index_to_dict, \
    from_pandas_index_dict_to_mongodb_query
from ckanext.mysql2mongodb.dataconv.validation import validator

from ckanext.mysql2mongodb.dataconv.constant.consts import SQL_FILE_EXTENSION, DATABASE_CHUNK_SIZE, INCORRECT_VALUE
from ckanext.mysql2mongodb.dataconv.constant.error_codes import TASK_PREPARE_DATA_ERROR, INPUT_FILE_EXTENSION_ERROR, \
    TASK_CONVERT_SCHEMA_ERROR, TASK_CONVERT_DATA_ERROR, TASK_DUMP_DATA_ERROR, TASK_UPLOAD_DATA_ERROR, \
    TASK_VALIDATE_DATA_ERROR, TASK_EXPORT_VALIDATOR_REPORT_ERROR
from ckanext.mysql2mongodb.dataconv.database.mongo_handler import MongoHandler
from ckanext.mysql2mongodb.dataconv.database.mysql_handler import MySQLHandler
from ckanext.mysql2mongodb.dataconv.exceptions import InvalidFileExtensionError, ValidationFlowIncompleteError
from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler
from ckanext.mysql2mongodb.settings import SAMPLE_PERCENTAGE

logger = logging.getLogger(__name__)


def prepare(sql_file_url: str, resource_id: str, sql_file_name: str):
    try:
        # check sql file type
        if sql_file_name.split('.')[-1] != SQL_FILE_EXTENSION:
            logger.error(f'error code: {INPUT_FILE_EXTENSION_ERROR}')
            raise InvalidFileExtensionError('Invalid MySQL backup file extension!')
        # region Main tasks
        file_system_handler.download_mysql_file_from_ckan(sql_file_url, resource_id, sql_file_name)
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
        mongo_handler.drop_db_if_exists(sql_file_name.split('.')[0])
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
        table_datatype_map = mongo_handler.get_table_datatype_map(db_name)
        for table_name in mongo_handler.get_table_name_list(db_name):
            data_generator = mysql_handler.fetch_data_for_mongo(db_name, table_name, table_datatype_map[table_name])
            for fetched_data in data_generator:
                converted_data = convert_mysql_to_mongodb(fetched_data, table_datatype_map[table_name])
                mongo_handler.store_data_to_collection(db_name, table_name, converted_data)
        # endregion
        logger.info('Task convert data success')
    except Exception as ex:
        logger.error(f'error code: {TASK_CONVERT_DATA_ERROR}')
        raise ex
    # endregion


def validate_data(resource_id: str, sql_file_name: str, package_id: str):
    try:
        mysql_handler = MySQLHandler()
        mongo_handler = MongoHandler()
        validator_log_handler = ValidatorLogHandler()
        db_name = sql_file_name.split('.')[0]

        table_primary_key_map = mongo_handler.get_table_primary_keys_map(db_name)
        table_name_list = mongo_handler.get_table_name_list(db_name)
        for table_name in table_name_list:
            try:
                validator.compare_total_rows(mysql_handler, mongo_handler, db_name, table_name)
                false_indexes = np.array([], dtype='object')
                for mysql_df in mysql_handler.to_pandas_dataframe(db_name, table_name, table_primary_key_map[table_name], chunksize=DATABASE_CHUNK_SIZE):
                    transform_mysql_df = mysql_df.applymap(func=transform_mysql_data_for_coreset_algorithm)
                    chosen_loc = lightweight_coreset(transform_mysql_df, round(len(mysql_df) * SAMPLE_PERCENTAGE))
                    sub_mysql_df = mysql_df.iloc[chosen_loc]
                    mongodb_query = from_pandas_index_dict_to_mongodb_query(
                        from_pandas_index_to_dict(sub_mysql_df.index)
                    )
                    sub_mongo_df = mongo_handler.to_pandas_dataframe(db_name, table_name, table_primary_key_map[table_name], mongodb_query)
                    false_indexes = np.append(false_indexes, validator.find_false_indexes(sub_mysql_df, sub_mongo_df))
                if false_indexes.size != 0:
                    raise ValidationFlowIncompleteError(INCORRECT_VALUE(false_indexes.size))
                logger.info(f'Validate database {db_name}, table {table_name} successfully')
            except ValidationFlowIncompleteError as ex:
                logger.info(f'Error found at database {db_name}, table {table_name}')
                validator_log_handler.write_log(
                    resource_id=resource_id,
                    package_id=package_id,
                    database=db_name,
                    table=table_name,
                    description=str(ex)
                )
                continue
        logger.info('Task validate data success')
    except Exception as ex:
        logger.error(f'error code: {TASK_VALIDATE_DATA_ERROR}')
        raise ex


def export_validator_report(resource_id: str, package_id: str):
    try:
        validator_log_handler = ValidatorLogHandler()
        validator_log_handler.export_validator_log_csv(resource_id=resource_id, package_id=package_id)
    except Exception as ex:
        logger.error(f'error code: {TASK_EXPORT_VALIDATOR_REPORT_ERROR}')
        raise ex


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
        file_system_handler.upload_mongo_dump_data_to_ckan(resource_id, sql_file_name, package_id)
        file_system_handler.upload_validator_report_to_ckan(resource_id, package_id)
        logger.info('Task upload data success')
    except Exception as ex:
        logger.error(f'error code: {TASK_UPLOAD_DATA_ERROR}')
        raise ex
