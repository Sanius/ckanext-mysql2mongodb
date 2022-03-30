import json
import logging
import subprocess
from datetime import datetime
from typing import Any, List, Dict

from bson import Decimal128

from ckanext.mysql2mongodb.dataconv.constant.consts import JSON_FILE_EXTENSION, MONGO_SCHEMA_COLLECTION, \
    MYSQL_MONGO_MAP, MONGO_DECIMAL_DATATYPE, MONGO_OBJECT_DATATYPE, MONGO_DUMP, GZIP_FILE_EXTENSION

from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler

from ckanext.mysql2mongodb.dataconv.exceptions import UnspecifiedDatabaseException, MongoCollectionNotFoundException

from ckanext.mysql2mongodb.dataconv.constant.error_codes import MONGO_UNSPECIFIED_DATABASE_ERROR, \
    MONGO_DATABASE_CONNECTION_ERROR, MONGO_DROP_DATABASE_ERROR, MONGO_IMPORT_SCHEMA_ERROR, \
    MONGO_DROP_COLLECTION_ERROR, MONGO_STORE_DATA_TO_COLLECTION_ERROR, MONGO_COLLECTION_NOT_FOUND_ERROR, \
    MONGO_EXTRACT_COLUMN_DATATYPE_ERROR, MONGO_CONVERT_DATA_ERROR, MONGO_DUMP_DATA_ERROR
from pymongo import MongoClient

from ckanext.mysql2mongodb.dataconv.database.abstract_database_handler import AbstractDatabaseHandler

from ckanext.mysql2mongodb.dataconv.settings import MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD, \
    MONGO_TOOL_ENV_VAR_PATH

logger = logging.getLogger(__name__)


class MongoHandler(AbstractDatabaseHandler):
    # Override
    def __init__(self):
        super().__init__()
        self._host = MONGO_HOST
        self._port = MONGO_PORT
        self._username = MONGO_USERNAME
        self._password = MONGO_PASSWORD

    # region Task methods
    def import_mysql_schema_json(self, resource_id: str, file_name: str):
        try:
            # region Get file path and file name
            schema_crawler_cache_dir = file_system_handler.create_schema_crawler_cache_dir(resource_id)
            db_name = file_name.split('.')[0]
            file_path = f'{schema_crawler_cache_dir}/{db_name}.{JSON_FILE_EXTENSION}'
            # endregion
            # region Get database connection
            self._set_db(db_name)
            self._drop_collection_if_exists(MONGO_SCHEMA_COLLECTION)
            # endregion
            with open(file_path) as file:
                json_data = json.load(file)
            self._store_data_to_collection(MONGO_SCHEMA_COLLECTION, json_data)
            logger.info(f'Write data from JSON file {db_name} to MongoDB collection MONGO_SCHEMA_COLLECTION of database {self._db} successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_IMPORT_SCHEMA_ERROR}')
            raise ex

    def get_schema_table_name_list(self, db_name: str) -> List:
        self._set_db(db_name)
        tables_schemas = self._get_flattened_collection_tables()
        remarks_tables_schema = list(filter(lambda table: not table.get('remarks'), tables_schemas))
        return list(map(lambda table: table['name'], remarks_tables_schema))

    def get_table_columnname_datatype(self, db_name: str) -> Dict:
        """
        Get dict of tables, columns name and columns data type.
        Dict(
                key: <table name>
                value: Dict(
                        key: <column name>
                        value: <MySQL column data type>
                )
        )
        """
        try:
            self._set_db(db_name)
            all_tables_schemas = self._get_flattened_collection_tables()
            all_columns = self._get_schema_collection_columns()

            columnid_tablename_map = {str(column): table_schema['name']
                                      for table_schema in all_tables_schemas
                                      for column in table_schema['columns']}

            columnid_datatype_map = {}
            for column in all_columns:
                column_datatype = column['column-data-type']
                if isinstance(column_datatype, dict):
                    columnid_datatype_map[column_datatype['@uuid']] = column_datatype['name'].split(' ')[0]

            table_name_list = list(map(lambda record: record['name'], all_tables_schemas))

            result = {k: {} for k in table_name_list}
            for column in all_columns:
                column_datatype = column['column-data-type']
                if isinstance(column_datatype, dict):
                    result[columnid_tablename_map[column['@uuid']]][column['name']] = columnid_datatype_map[column_datatype['@uuid']]
                else:
                    result[columnid_tablename_map[column['@uuid']]][column['name']] = columnid_datatype_map[column_datatype]
            return result
        except Exception as ex:
            logger.error(f'error code: {MONGO_EXTRACT_COLUMN_DATATYPE_ERROR}')
            raise ex

    def store_data_to_collection(self, db_name: str, table_name: str, data: Any):
        self._set_db(db_name)
        self._store_data_to_collection(table_name, data)

    def dump_database(self, resource_id: str, sql_file_name: str):
        try:
            mongo_dump_data_dir = file_system_handler.get_mongo_dump_cache_path(resource_id)
            db_name = sql_file_name.split('.')[0]
            file_name = f'{db_name}.{GZIP_FILE_EXTENSION}'
            file_system_handler.create_mongo_dump_cache_dir(resource_id)
            self._set_db(db_name)
            self._dump_database(mongo_dump_data_dir, file_name)
            logger.info('Dump data successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_DUMP_DATA_ERROR}')
            raise ex

    def drop_old_db(self, db_name):
        self._set_db(db_name)
        self._drop_db()
    # endregion

    # region Schema crawler methods
    def _get_flattened_collection_tables(self) -> List:
        _SELECTED_KEY_SET = ('@uuid', 'name', 'columns', 'remarks')
        _TABLE_TYPES = ('foreign-key-table', 'primary-key-table')

        def flatten(key_collections: List) -> List:
            table_list = []
            for collection in list(filter(lambda collection: isinstance(collection, dict), key_collections)):
                for table_type in _TABLE_TYPES:
                    if collection.get(table_type) and isinstance(collection[table_type], dict):
                        table_list = [
                            *table_list,
                            {k: collection[table_type][k] for k in _SELECTED_KEY_SET},
                            *flatten(collection[table_type]['foreign-keys'])
                        ]
            return table_list

        schemas_collections = self._get_schema_collection()
        table_list = []
        for schema_collection in list(filter(lambda collection: isinstance(collection, dict), schemas_collections['catalog']['tables'])):
            table_list = [*table_list,
                          {k: schema_collection[k] for k in _SELECTED_KEY_SET},
                          *flatten(schema_collection['foreign-keys'])]
        return table_list

    def _get_schema_collection_tables(self) -> List:
        schema_collection = self._get_schema_collection()
        return list(filter(lambda record: isinstance(record, dict), schema_collection['catalog']['tables']))

    def _get_schema_collection_columns(self) -> List:
        schema_collection = self._get_schema_collection()
        return list(filter(lambda record: isinstance(record, dict), schema_collection['all-table-columns']))

    def _get_schema_collection(self) -> Any:
        if not self._does_collection_exist(MONGO_SCHEMA_COLLECTION):
            logger.error(f'error code: {MONGO_COLLECTION_NOT_FOUND_ERROR}')
            raise MongoCollectionNotFoundException(f'collection {MONGO_SCHEMA_COLLECTION} not found')
        schema_collection = self._get_db_connection()[MONGO_SCHEMA_COLLECTION]
        return schema_collection.find()[0]
    # endregion

    # region Middleware methods
    def _store_data_to_collection(self, collection_name: str, collection_data: Any):
        db = self._get_db_connection()
        try:
            if isinstance(collection_data, list):
                db[collection_name].insert_many(collection_data)
            else:
                db[collection_name].insert_one(collection_data)
            logger.info(f'Write JSON data to Datastore collection {collection_name} successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_STORE_DATA_TO_COLLECTION_ERROR}')
            raise ex

    def _drop_db(self):
        if not self._db:
            logger.error(f'error code: {MONGO_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        try:
            # Making connection
            conn = self._get_open_connection()
            conn.drop_database(self._db)
            logger.info(f'Drop database successfully')
        except Exception as ex:
            logger.error(f'error code: {MONGO_DROP_DATABASE_ERROR}')
            raise ex

    def _drop_collection_if_exists(self, collection_name: str):
        try:
            db = self._get_db_connection()
            if self._does_collection_exist(collection_name):
                db.drop_collection(collection_name)
                logger.info(f'Drop collection successfully')
            else:
                logger.info(f'Collection {collection_name} does not exist in {self._db}')
        except Exception as ex:
            logger.error(f'error code: {MONGO_DROP_COLLECTION_ERROR}')
            raise ex

    def _does_collection_exist(self, collection_name: str) -> bool:
        db = self._get_db_connection()
        return False if not collection_name else db[collection_name].count_documents({'_id': {'$exists': True}}) == 1
    # endregion

    # region Component methods
    def _dump_database(self, dump_data_dir: str, file_name: str):
        if not self._db:
            logger.error(f'error code: {MONGO_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        file_path = f'{dump_data_dir}/{file_name}'
        mongodump_command = f'{MONGO_TOOL_ENV_VAR_PATH}/{MONGO_DUMP}'
        command_line_str = '''
            {command} --username={mongo_user} \
            --password={mongo_password} \
            --host={mongo_host} \
            --port={mongo_port} \
            --db={mongo_database} \
            --authenticationDatabase=admin \
            --authenticationMechanism=SCRAM-SHA-256 \
            --forceTableScan \
            --gzip \
            --archive={mongo_dump_file_path} \
        '''.format(
            command=mongodump_command,
            mongo_user=self._username,
            mongo_password=self._password,
            mongo_database=self._db,
            mongo_host=self._host,
            mongo_port=self._port,
            mongo_dump_file_path=file_path
        )
        subprocess.run([command_line_str], check=True, shell=True)

    # Override
    def _set_db(self, db: str):
        self._db = db

    def _get_db_connection(self) -> Any:
        if not self._db:
            logger.error(f'error code: {MONGO_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        conn = self._get_open_connection()
        return conn[self._db]

    # Override
    # Does not have to _set_db first
    def _get_open_connection(self) -> Any:
        uri = 'mongodb://{username}:{password}@{host}:{port}/?authMechanism=SCRAM-SHA-256' \
            .format(username=self._username, password=self._password, host=self._host, port=self._port)
        try:
            # Making connection
            return MongoClient(uri)
        except Exception as ex:
            logger.error(f'error code: {MONGO_DATABASE_CONNECTION_ERROR}')
            raise ex
    # endregion

