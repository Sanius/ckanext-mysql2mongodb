import json
import logging
import subprocess
from collections import OrderedDict
from typing import List, Dict, Union
import typing

import pandas as pd

from ckanext.mysql2mongodb.dataconv.constant.consts import JSON_FILE_EXTENSION, MONGO_SCHEMA_COLLECTION, \
    MONGO_DUMP, GZIP_FILE_EXTENSION

from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler

from ckanext.mysql2mongodb.dataconv.exceptions import MongoCollectionNotFoundException, MongoDatabaseNotFoundException, \
    UnspecifiedDatabaseException

from ckanext.mysql2mongodb.dataconv.constant.error_codes import MONGO_DATABASE_CONNECTION_ERROR, \
    MONGO_DROP_DATABASE_ERROR, MONGO_IMPORT_SCHEMA_ERROR, MONGO_DROP_COLLECTION_ERROR, \
    MONGO_STORE_DATA_TO_COLLECTION_ERROR, MONGO_COLLECTION_NOT_FOUND_ERROR, \
    MONGO_EXTRACT_COLUMN_DATATYPE_ERROR, MONGO_DUMP_DATA_ERROR, MONGO_DATABASE_NOT_FOUND_ERROR, \
    MONGO_UNSPECIFIED_DATABASE_ERROR
from pymongo import MongoClient
from pymongo.database import Database

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
            self._drop_collection_if_exists(db_name, MONGO_SCHEMA_COLLECTION)
            # endregion
            with open(file_path) as file:
                json_data = json.load(file)
            self._store_data_to_collection(db_name, MONGO_SCHEMA_COLLECTION, json_data)
            logger.info(
                f'Write data from JSON file {db_name} to MongoDB collection MONGO_SCHEMA_COLLECTION of database {db_name} successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_IMPORT_SCHEMA_ERROR}')
            raise ex

    def dump_database(self, resource_id: str, sql_file_name: str):
        try:
            db_name = sql_file_name.split('.')[0]
            file_system_handler.create_mongo_dump_cache_dir(resource_id)
            mongo_dump_data_dir = file_system_handler.get_mongo_dump_cache_path(resource_id)
            self._dump_database(db_name, mongo_dump_data_dir)
            logger.info('Dump data successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_DUMP_DATA_ERROR}')
            raise ex

    def store_data_to_collection(self, db_name: str, table_name: str, data: Union[List, Dict]):
        self._store_data_to_collection(db_name, table_name, data)

    def drop_db_if_exists(self, db_name: str):
        self._drop_db_if_exists(db_name)

    def get_table_datatype_map(self, db_name: str) -> Dict:
        """
        Get dict of tables, columns name and columns data type.
        Dict(
                key: <table name>
                value: List({
                        column_name: <column name>
                        column_datatype: <MySQL column data type>
                })
        )
        """
        try:
            return self._get_table_schema_dataframe(db_name).groupby(by='table_name', sort=False) \
                .apply(lambda df: df[['column_name', 'column_datatype']].to_dict(orient='records')) \
                .to_dict()
        except Exception as ex:
            logger.error(f'error code: {MONGO_EXTRACT_COLUMN_DATATYPE_ERROR}')
            raise ex

    # def to_pandas_dataframe(self, db_name: str, collection_name: str) -> pd.DataFrame:
    #     try:
    #         if not self.does_collection_exists(db_name, collection_name):
    #             logger.error(f'error code: {MONGO_COLLECTION_NOT_FOUND_ERROR}')
    #             raise MongoCollectionNotFoundException(f'collection {MONGO_SCHEMA_COLLECTION} not found')
    #         db = self._get_db_connection(db_name)
    #         return pd.DataFrame(db[collection_name].find())
    #     except Exception as ex:
    #         logger.error(f'error code: {MONGO_UNABLE_TO_CREATE_PANDAS_DATAFRAME_ERROR}')
    #         raise ex
    # endregion

    # region Schema crawler methods
    def _get_table_schema_dataframe(self, db_name: str) -> pd.DataFrame:
        """
        ____________________________________________________________
        | column_uuid | table_name | column_name | column_datatype |
        | (index)     |            |             |                 |
        ____________________________________________________________
        |             |            |             |                 |
        |             |            |             |                 |
        |             |            |             |                 |
        ____________________________________________________________
        """
        def get_column_tablename() -> typing.OrderedDict:
            """
            { column_uuid: table_name }
            """
            all_tables = self._get_schema_collection_tables_flattened(db_name)
            return OrderedDict(
                (str(column_uuid), table['name'])
                for table in all_tables
                for column_uuid in table['columns']
            )

        def get_column_name() -> typing.OrderedDict:
            """
            { column_uuid: column_name }
            """
            all_columns = self._get_schema_collection_columns(db_name)
            return OrderedDict((column['@uuid'], column['name']) for column in all_columns)

        def get_column_datatype() -> Dict:
            """
            { column_uuid: column_datatype_name }
            """
            all_columns = self._get_schema_collection_columns(db_name)
            datatype_uuid_name_map = {}
            column_uuid_datatypeuuid = {}
            for column in all_columns:
                datatype = column['column-data-type']
                datatype_uuid = datatype
                if isinstance(datatype, dict):
                    datatype_uuid = datatype['@uuid']
                    datatype_uuid_name_map[datatype['@uuid']] = datatype['name'].split(' ')[0]
                column_uuid_datatypeuuid[column['@uuid']] = datatype_uuid

            return {column_uuid: datatype_uuid_name_map[datatype_uuid]
                    for column_uuid, datatype_uuid in column_uuid_datatypeuuid.items()}

        schema_list = []
        column_tablename = get_column_tablename()
        column_datatype = get_column_datatype()
        column_name = get_column_name()
        for column_uuid in column_tablename:
            schema_list.append({
                'column_uuid': column_uuid,
                'table_name': column_tablename.get(column_uuid),
                'column_name': column_name.get(column_uuid),
                'column_datatype': column_datatype.get(column_uuid),
            })
        return pd.DataFrame(schema_list).set_index('column_uuid')

    def get_table_name_list(self, db_name: str) -> List:
        db_schema = self._get_schema_collection_tables_flattened(db_name)
        tables_not_views = list(filter(lambda table: not table.get('remarks'), db_schema))
        return list(map(lambda table: table['name'], tables_not_views))

    def _get_schema_collection_tables_flattened(self, db_name: str) -> List:
        _SELECTED_KEY_SET = ('@uuid', 'name', 'columns', 'remarks')
        _TABLE_TYPES = ('foreign-key-table', 'primary-key-table')

        def flatten(key_collections: List) -> List:
            table_list_inner = []
            for collection in list(filter(lambda collection: isinstance(collection, dict), key_collections)):
                for table_type in _TABLE_TYPES:
                    if collection.get(table_type) and isinstance(collection[table_type], dict):
                        table_list_inner = [
                            *table_list_inner,
                            {k: collection[table_type][k] for k in _SELECTED_KEY_SET},
                            *flatten(collection[table_type]['foreign-keys'])
                        ]
            return table_list_inner

        schemas_collections = self._get_schema_collection(db_name)
        table_list = []
        for schema_collection in list(filter(lambda collection: isinstance(collection, dict), schemas_collections['catalog']['tables'])):
            table_list = [*table_list,
                          {k: schema_collection[k] for k in _SELECTED_KEY_SET},
                          *flatten(schema_collection['foreign-keys'])]
        return table_list

    def _get_schema_collection_tables(self, db_name: str) -> List:
        schema_collection = self._get_schema_collection(db_name)
        return list(filter(lambda record: isinstance(record, dict), schema_collection['catalog']['tables']))

    def _get_schema_collection_columns(self, db_name: str) -> List:
        schema_collection = self._get_schema_collection(db_name)
        return list(filter(lambda record: isinstance(record, dict), schema_collection['all-table-columns']))

    def _get_schema_collection(self, db_name: str) -> Dict:
        if not self.does_collection_exists(db_name, MONGO_SCHEMA_COLLECTION):
            logger.error(f'error code: {MONGO_COLLECTION_NOT_FOUND_ERROR}')
            raise MongoCollectionNotFoundException(f'collection {MONGO_SCHEMA_COLLECTION} not found')
        schema_collection = self._get_db_connection(db_name)[MONGO_SCHEMA_COLLECTION]
        return schema_collection.find()[0]
    # endregion

    # region Middleware methods
    def _dump_database(self, db_name: str, dump_data_dir: str):
        if not db_name or not self._does_db_exist(db_name):
            logger.error(f'error code: {MONGO_DATABASE_NOT_FOUND_ERROR}')
            raise MongoDatabaseNotFoundException('Database not found')
        file_path = f'{dump_data_dir}/{db_name}.{GZIP_FILE_EXTENSION}'
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
            mongo_database=db_name,
            mongo_host=self._host,
            mongo_port=self._port,
            mongo_dump_file_path=file_path
        )
        subprocess.run([command_line_str], check=True, shell=True)

    def _store_data_to_collection(self, db_name: str, collection_name: str, collection_data: Union[List, Dict]):
        try:
            db = self._get_db_connection(db_name)
            if isinstance(collection_data, list):
                db[collection_name].insert_many(collection_data)
            else:
                db[collection_name].insert_one(collection_data)
            logger.info(f'Write JSON data to Datastore collection {collection_name} successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_STORE_DATA_TO_COLLECTION_ERROR}')
            raise ex

    def _drop_collection_if_exists(self, db_name: str, collection_name: str):
        try:
            db = self._get_db_connection(db_name)
            if self.does_collection_exists(db_name, collection_name):
                db.drop_collection(collection_name)
                logger.info(f'Drop collection successfully')
            else:
                logger.info(f'Collection {collection_name} does not exist in {db_name}')
        except Exception as ex:
            logger.error(f'error code: {MONGO_DROP_COLLECTION_ERROR}')
            raise ex

    # endregion

    # region Component methods
    def does_collection_exists(self, db_name: str, collection_name: str) -> bool:
        db = self._get_db_connection(db_name)
        return False if not collection_name else db[collection_name].count_documents({'_id': {'$exists': True}}) == 1

    def _drop_db_if_exists(self, db_name: str):
        try:
            mongo_client = self._get_open_connection()
            mongo_client.drop_database(db_name)
            logger.info(f'Drop database successfully')
        except Exception as ex:
            logger.error(f'error code: {MONGO_DROP_DATABASE_ERROR}')
            raise ex

    def _get_db_connection(self, db_name: str) -> Database:
        if not db_name:
            logger.error(f'error code: {MONGO_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Database name is incorrect')
        return self._get_open_connection()[db_name]
    # endregion

    # region Inheritance methods
    # Override
    def _does_db_exist(self, db_name: str) -> bool:
        if not db_name:
            return False
        client = self._get_open_connection()
        return db_name in client.list_database_names()

    # Override
    def _get_open_connection(self) -> MongoClient:
        uri = 'mongodb://{username}:{password}@{host}:{port}/?authMechanism=SCRAM-SHA-256' \
            .format(username=self._username, password=self._password, host=self._host, port=self._port)
        try:
            return MongoClient(uri)
        except Exception as ex:
            logger.error(f'error code: {MONGO_DATABASE_CONNECTION_ERROR}')
            raise ex
    # endregion
