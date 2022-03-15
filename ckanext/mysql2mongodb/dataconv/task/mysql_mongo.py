import logging

from ckanext.mysql2mongodb.dataconv.database import database_factory

from ckanext.mysql2mongodb.dataconv.database.mysql_handler import MySQLHandler

from ckanext.mysql2mongodb.dataconv.exceptions import InvalidFileExtensionError

from ckanext.mysql2mongodb.dataconv.util import command_lines

from ckanext.mysql2mongodb.dataconv.constant.error_codes import TASK_PREPARE_DATA_ERROR, INPUT_FILE_EXTENSION_ERROR

from ckanext.mysql2mongodb.dataconv.constant.consts import SQL_FILE_EXTENSION, MYSQL

logger = logging.getLogger(__name__)


def prepare(sql_file_url: str, resource_id: str, sql_file_name: str):
    try:
        # check sql file type
        if sql_file_name.split('.')[-1] != SQL_FILE_EXTENSION:
            logger.error(f'error code: {INPUT_FILE_EXTENSION_ERROR}')
            raise InvalidFileExtensionError('Invalid MySQL backup file extension!')

        # get sql bak
        command_lines.download_from_ckan_mysql_file(sql_file_url, resource_id, sql_file_name)
        # process mysql
        mysql_handler = database_factory.produce_database(MYSQL)
        mysql_handler.restore_from_ckan(resource_id, sql_file_name)
    except Exception as ex:
        logger.error(f'error code: {TASK_PREPARE_DATA_ERROR}')
        raise ex


def converse_schema():
    # try:
    #     _, _, _, db_conf, schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname = pull_from_xcom(
    #         kwargs)
    #
    #     os.system("whoami")
    #     # LOCATION = "/srv/app/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
    #     LOCATION = "/usr/lib/ckan/default/src/ckanext-mysql2mongodb/ckanext/mysql2mongodb/data_conv"
    #     os.chdir(LOCATION)
    #
    #     schema_conv_init_option = ConvInitOption(
    #         host=mysql_host, username=mysql_username, password=mysql_password, port=mysql_port, dbname=mysql_dbname)
    #
    #     mongodb_host = db_conf["mongodb_host"]
    #     mongodb_username = db_conf["mongodb_username"]
    #     mongodb_password = db_conf["mongodb_password"]
    #     mongodb_port = db_conf["mongodb_port"]
    #     mongodb_dbname = schema_name
    #
    #     schema_conv_output_option = ConvOutputOption(
    #         host=mongodb_host, username=mongodb_username, password=mongodb_password, port=mongodb_port, dbname=mongodb_dbname)
    #
    #     schema_conversion = SchemaConversion()
    #     schema_conversion.set_config(
    #         schema_conv_init_option, schema_conv_output_option)
    #     schema_conversion.run()
    #
    #     kwargs['ti'].xcom_push(key='schema_conv_init_option',
    #                            value=jsonpickle.encode(schema_conv_init_option))
    #     kwargs['ti'].xcom_push(key='schema_conv_output_option',
    #                            value=jsonpickle.encode(schema_conv_output_option))
    #     kwargs['ti'].xcom_push(key='schema_conversion',
    #                            value=jsonpickle.encode(schema_conversion))
    #
    # except Exception as exception:
    #     logger.error("Error Occure in taskDataConv Task!")
    #     logger.error(str(exception))
    #     raise exception
    pass


def converse_data():
    logger.info('=========================== Converse Schema ===========================')


def upload_converted_data():
    logger.info('=========================== Load Result ===========================')
