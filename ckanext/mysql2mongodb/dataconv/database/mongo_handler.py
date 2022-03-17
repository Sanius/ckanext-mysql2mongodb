import json
import logging
from typing import Any, List, Dict

from ckanext.mysql2mongodb.dataconv.constant.consts import JSON_FILE_EXTENSION, MONGO_SCHEMA_COLLECTION

from ckanext.mysql2mongodb.dataconv.util import command_lines

from ckanext.mysql2mongodb.dataconv.exceptions import UnspecifiedDatabaseException, MongoCollectionNotFoundException

from ckanext.mysql2mongodb.dataconv.constant.error_codes import MONGO_UNSPECIFIED_DATABASE_ERROR, \
    MONGO_DATABASE_CONNECTION_ERROR, MONGO_DROP_DATABASE_ERROR, MONGO_IMPORT_SCHEMA_ERROR, \
    MONGO_DROP_COLLECTION_ERROR, MONGO_STORE_DATA_TO_COLLECTION_ERROR, MONGO_COLLECTION_NOT_FOUND
from pymongo import MongoClient

from ckanext.mysql2mongodb.dataconv.database.abstract_database_handler import AbstractDatabaseHandler

from ckanext.mysql2mongodb.dataconv.settings import MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD

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
            schema_crawler_cache_dir = command_lines.create_schema_crawler_cache_dir(resource_id)
            db_name = file_name.split('.')[0]
            file_path = f'{schema_crawler_cache_dir}/{db_name}.{JSON_FILE_EXTENSION}'
            # endregion
            # region Get database connection
            self.set_db(db_name)
            self._drop_collection_if_exists(MONGO_SCHEMA_COLLECTION)
            # endregion
            with open(file_path) as file:
                json_data = json.load(file)
            self._store_data_to_collection(MONGO_SCHEMA_COLLECTION, json_data)
            logger.info(
                f'Write data from JSON file {db_name} to MongoDB collection MONGO_SCHEMA_COLLECTION of database {self._db} successfully!')
        except Exception as ex:
            logger.error(f'error code: {MONGO_IMPORT_SCHEMA_ERROR}')
            raise ex

    def convert_relations_to_references(self):
        """
        Convert relations of MySQL table to database references of MongoDB
        """
        tables_name_list = self.schema.get_tables_name_list()
        # db_connection = open_connection_mongodb(mongodb_connection_info)
        tables_relations = self.schema.get_tables_relations()
        # converting_tables_order = specify_sequence_of_migrating_tables(schema_file)
        edited_table_relations_dict = {}
        original_tables_set = set(
            [tables_relations[key]["primary_key_table"] for key in tables_relations])

        # Edit relations of table dictionary
        for original_table in original_tables_set:
            for key in tables_relations:
                if tables_relations[key]["primary_key_table"] == original_table:
                    if original_table not in edited_table_relations_dict.keys():
                        edited_table_relations_dict[original_table] = []
                    edited_table_relations_dict[original_table] = edited_table_relations_dict[original_table] + [
                        extract_dict(["primary_key_column", "foreign_key_table", "foreign_key_column"])(tables_relations[key])]
        # Convert each relation of each table
        for original_collection_name in tables_name_list:
            if original_collection_name in original_tables_set:
                for relation_detail in edited_table_relations_dict[original_collection_name]:
                    referencing_collection_name = relation_detail["foreign_key_table"]
                    original_key = relation_detail["primary_key_column"]
                    referencing_key = relation_detail["foreign_key_column"]
                    self.convert_one_relation_to_reference(
                        original_collection_name, referencing_collection_name, original_key, referencing_key)
        print("Convert relations successfully!")

    def convert_one_relation_to_reference(self, original_collection_name, referencing_collection_name, original_key, referencing_key):
        """
        Convert one relation of MySQL table to database reference of MongoDB
        """
        db_connection = open_connection_mongodb(
            self.schema_conv_output_option)
        original_collection_connection = db_connection[original_collection_name]
        original_documents = original_collection_connection.find()
        new_referenced_key_dict = {}
        for doc in original_documents:
            new_referenced_key_dict[doc[original_key]] = doc["_id"]

        referencing_documents = db_connection[referencing_collection_name]
        for key in new_referenced_key_dict:
            new_reference = {}
            new_reference["$ref"] = original_collection_name
            new_reference["$id"] = new_referenced_key_dict[key]
            new_reference["$db"] = self.schema_conv_output_option.dbname
            referencing_key_new_name = "db_ref_" + referencing_key
            referencing_documents.update_many({referencing_key: key}, update={
                                              "$set": {referencing_key_new_name: new_reference}})

    # def create_schema_view_db(self):
    #     """
    #     Store a MySQL converted schema in MongoDB.
    #     This schema will be used for generated detail schema in future by end-user.
    #     Converted schema structure:
    #         Dict(
    #             "Converted schema": Dict(
    #                 "Database type": "MySQL",
    #                 "Schema": <Database name>,
    #                 "Tables": List[
    #                     Dict(
    #                         "Table name": <table name>,
    #                         "Columns": List[
    #                             "Column name": <column name>
    #                         ]
    #                     )
    #                 ]
    #             )
    #         )
    #     """
    #     try:
    #         collection = self._get_db_connection()[MONGO_SCHEMA_COLLECTION]
    #         db_schema = collection.find()[0]
    #         catalog_schema = db_schema['catalog']
    #         columns_schema = db_schema['all-table-columns']
    #         tables_schema = catalog_schema['tables']
    #         converted_schema = {
    #             'database-name': catalog_schema['database-info']['product-name'],
    #             'database-version': catalog_schema['database-info']['product-version'],
    #             'schema': catalog_schema['name'],
    #             'tables': [],
    #             'foreign-keys': []
    #         }
    #         # region tables dict
    #         tables_dict = {}
    #         for table in tables_schema:
    #             if type(table) is dict:
    #                 for col in table['columns']:
    #                     tables_dict[str(col)] = table['name']
    #         # endregion
    #         # region cols dict
    #         cols_dict = {}
    #         for col in columns_schema:
    #             cols_dict[col['@uuid']] = col['name']
    #         # endregion
    #         for table_schema in tables_schema:
    #             if type(table_schema) is not dict:
    #                 continue
    #             # region Get tables info
    #             table_info = {
    #                 'name': table_schema['name'],
    #                 'engine': table_schema['attributes']['ENGINE'],
    #                 'table-collation': table_schema['attributes']['TABLE_COLLATION'],
    #                 'constraints': [],
    #                 'triggers': [],
    #                 'columns': [],
    #                 'indexes': []
    #             }
    #             for table_schema_constraint in table_schema['table-constraints']:
    #                 if type(table_schema_constraint) is dict:
    #                     table_constraint = {
    #                         'name': table_schema_constraint['name'],
    #                         'type': table_schema_constraint['constraint-type'],
    #                         'definition': table_schema_constraint['definition']
    #                     }
    #                     table_info['constraints'].append(table_constraint)
    #
    #             for table_schema_trigger in table_schema['triggers']:
    #                 if type(table_schema_trigger) is dict:
    #                     table_trigger = {
    #                         'name': table_schema_trigger['name'],
    #                         'action-condition': table_schema_trigger['action-condition'],
    #                         'action-order': table_schema_trigger['action-order'],
    #                         'action-orientation': table_schema_trigger['action-orientation'],
    #                         'action-statement': table_schema_trigger['action-statement'],
    #                         'condition-timing': table_schema_trigger['condition-timing'],
    #                         'event-manipulation-type': table_schema_trigger['event-manipulation-type'],
    #                     }
    #                     table_info['triggers'].append(table_trigger)
    #
    #             for column_schema in columns_schema:
    #                 if column_schema['@uuid'] in table_schema['columns']:
    #                     column_info = {
    #                         'name': column_schema['name'],
    #                         'character-set-name': column_schema['attributes']['CHARACTER_SET_NAME'],
    #                         'collation-name': column_schema['attributes']['COLLATION_NAME'],
    #                         'column-type': column_schema['attributes']['COLUMN_TYPE'],
    #                         'is-nullable': column_schema['attributes']['IS_NULLABLE'],
    #                         'auto-incremented': column_schema['auto-incremented'],
    #                         'nullable': column_schema['nullable'],
    #                         'default-value': column_schema['default-value'],
    #                     }
    #                     table_info['columns'].append(column_info)
    #
    #             for index_schema in table_schema['indexes']:
    #                 if type(index_schema) is dict:
    #                     index_column_list = list(
    #                         map(
    #                             lambda col_sche: {'name': col_sche['name'], 'table': col_sche['short-name'].split('.')[0]},
    #                             list(
    #                                 filter(
    #                                     lambda col_sche: col_sche['@uuid'] in index_schema['columns'], columns_schema
    #                                 )
    #                             )
    #                         )
    #                     )
    #                     index_info = {
    #                         'name': index_schema['name'],
    #                         'unique': index_schema['unique'],
    #                         'columns': index_column_list
    #                     }
    #                     table_info['indexes'].append(index_info)
    #
    #             converted_schema['tables'].append(table_info)
    #             # endregion
    #             # region Get foreign-keys info
    #             for foreign_key_schema in table_schema['foreign-keys']:
    #                 if type(foreign_key_schema) is dict:
    #                     col_refs = list(
    #                         map(
    #                             lambda fk_sche: {
    #                                 'key-sequence': fk_sche['key-sequence'],
    #                                 'foreign-key-column': cols_dict[fk_sche['foreign-key-column']],
    #                                 'foreign-key-table': tables_dict.get(fk_sche['foreign-key-column']),
    #                                 'primary-key-column': cols_dict[fk_sche['primary-key-column']],
    #                                 'primary-key-table': tables_dict[fk_sche['primary-key-column']],
    #                             }, foreign_key_schema['column-references']
    #                         )
    #                     )
    #                     foreign_key_info = {
    #                         'name': foreign_key_schema['name'],
    #                         'column-references': col_refs,
    #                         'delete-rule': foreign_key_schema['delete-rule'],
    #                         'update-rule': foreign_key_schema['update-rule'],
    #                     }
    #                     converted_schema['foreign-keys'].append(foreign_key_info)
    #             # endregion
    #
    #         # region Save to schema_view database
    #         self._drop_collection_if_exists(MONGO_SCHEMA_VIEW_COLLECTION)
    #         collection = self._get_db_connection()[MONGO_SCHEMA_VIEW_COLLECTION]
    #         if isinstance(converted_schema, list):
    #             collection.insert_many(converted_schema)
    #         else:
    #             collection.insert_one(converted_schema)
    #         # endregion
    #         logger.info(f'Save schema view from {self._db} database to MongoDB successfully')
    #     except Exception as ex:
    #         logger.error(f'error code: {MONGO_CREATE_SCHEMA_VIEW_ERROR}')
    #         raise ex

    # def create_schema_validators(self):
    #     """
    #     Specify MongoDB schema validator for all tables.
    #     """
    #     table_view_column_dtype = self.get_table_column_and_data_type()
    #     table_list = self.get_tables_name_list()
    #     uuid_col_dict = self.get_columns_dict()
    #     table_column_dtype = {}
    #     for table in table_list:
    #         table_column_dtype[table] = table_view_column_dtype[table]
    #     table_cols_uuid = {}
    #     for table in self.tables_schema:
    #         table_name = table["name"]
    #         if table_name in table_list:
    #             table_cols_uuid[table_name] = table["columns"]
    #     enum_col_dict = {}
    #     for col in self.all_table_columns:
    #         if col["attributes"]["COLUMN_TYPE"][:4] == "enum":
    #             data = {}
    #             table_name, col_name = col["short-name"].split(".")[:2]
    #             if table_name in table_list:
    #                 data = list(map(lambda ele: ele[1:-1], col["attributes"]["COLUMN_TYPE"][5:-1].split(",")))
    #                 sub_dict = {}
    #                 sub_dict[col_name] = data
    #                 enum_col_dict[table_name] = sub_dict
    #     db_connection = open_connection_mongodb(self.schema_conv_output_option.host,
    #                                             self.schema_conv_output_option.port,
    #                                             self.schema_conv_output_option.dbname)
    #     for table in self.get_tables_and_views_list():
    #         db_connection.create_collection(table)
    #     for table in table_cols_uuid:
    #         props = {}
    #         for col_uuid in table_cols_uuid[table]:
    #             col_name = uuid_col_dict[col_uuid]
    #             mysql_dtype = table_column_dtype[table][col_name]
    #             if mysql_dtype == "ENUM":
    #                 data = {
    #                     "enum": enum_col_dict[table][col_name],
    #                     "description": "can only be one of the enum values"
    #                 }
    #             else:
    #                 data = {
    #                     "bsonType": self.data_type_schema_mapping(mysql_dtype)
    #                 }
    #             props[col_name] = data
    #             json_schema = {}
    #             json_schema["bsonType"] = "object"
    #             json_schema["properties"] = props
    #             # print(json_schema)
    #             vexpr = {"$jsonSchema": json_schema}
    #             cmd = OrderedDict([('collMod', table), ('validator', vexpr)])
    #             db_connection.command(cmd)

    def get_schema_table_name_list(self, db_name: str) -> List:
        self.set_db(db_name)
        tables_schema = self._get_schema_collection_tables()
        remark_tables_schema = list(filter(lambda table: table.get('remarks') == '', tables_schema))
        return list(map(lambda table: table['name'], remark_tables_schema))

    def get_table_column_and_data_type(self, db_name: str) -> Dict:
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
        def get_table_dict() -> Dict:
            """
            Extract column uuid and its table name from database schema
            Return a dictionary with @uuid as key and table name as value
            Dict(key: <column uuid>, value: <name of table has that column>)
            """
            return_dict = {}
            for table in tables_schema:
                for col in table['columns']:
                    return_dict[str(col)] = table['name']
            return return_dict

        def get_table_and_view_list() -> List[Any]:
            """
            Get list of name of all tables and views.
            """
            return list(map(lambda record: record['name'], tables_schema))
        self.set_db(db_name)
        tables_schema = self._get_schema_collection_tables()
        all_columns = self._get_schema_collection_columns()

        table_dict = get_table_dict()
        schema_type_dict = {}
        for column in all_columns:
            datatype = column['column-data-type']
            if isinstance(datatype, dict):
                schema_type_dict[datatype['@uuid']] = datatype['name']

        table_list = get_table_and_view_list()

        result = {}
        for table_name in table_list:
            result[table_name] = {}
        for column in all_columns:
            datatype = column['column-data-type']
            try:
                if isinstance(datatype, dict):
                    result[table_dict[column['@uuid']]][column['name']] = schema_type_dict[datatype['@uuid']]
                else:
                    result[table_dict[column['@uuid']]][column['name']] = schema_type_dict[datatype]
            except KeyError:
                continue
        return result

    def store_fetched_mysql_data(self, db_name: str, table_name: str, fetched_data: List):
        """
        Parallel
        """
        colname_coltype_dict = self.schema.get_table_column_and_data_type()[
            table_name]
        rows = []
        # Parallel start from here
        for row in fetched_data:
            data = {}
            col_fetch_seq = list(colname_coltype_dict.keys())
            for i in range(len(col_fetch_seq)):
                col = col_fetch_seq[i]
                dtype = colname_coltype_dict[col]
                target_dtype = self.find_converted_dtype(dtype)
                # generate SQL
                cell_data = row[i]
                if cell_data != None:
                    # if dtype == "GEOMETRY":
                    # 	geodata = [float(num) for num in cell_data[6:-1].split()]
                    # 	geo_x, geo_y = geodata[:2]
                    # 	if geo_x > 180 or geo_x < -180:
                    # 		geo_x = 0
                    # 	if geo_y > 90 or geo_y < -90:
                    # 		geo_y = 0
                    # 	converted_data = {
                    # 		"type": "Point",
                    # 		"coordinates": [geo_x, geo_y]
                    # 	}
                    if dtype == "GEOMETRY":
                        converted_data = cell_data
                    if dtype == "VARBINARY":
                        # print(type(cell_data), str(cell_data))
                        converted_data = bytes(cell_data)
                        # print(type(converted_data), converted_data)
                        # return
                    elif dtype == "VARCHAR":
                        # print(str[cell_data], type(cell_data))
                        # return
                        converted_data = str(cell_data)
                    elif dtype == "BIT":
                        # get col type from schema attribute
                        # mysql_col_type = self.schema.get_col_type_from_schema_attribute(table, col)
                        # if mysql_col_type == "tinyint(1)":
                        # 	binary_num = cell_data
                        # 	converted_data = binary_num.to_bytes(len(str(binary_num)), byteorder="big")
                        # else:
                        # 	converted_data = cell_data
                        converted_data = cell_data
                    # elif dtype == "YEAR":
                        # print(cell_data, type(cell_data))
                    elif dtype == "DATE":
                        # print(cell_data, type(cell_data))
                        # , cell_data.hour, cell_data.minute, cell_data.second)
                        converted_data = datetime(
                            cell_data.year, cell_data.month, cell_data.day)
                    # elif dtype == "JSON":
                        # print(type(cell_data), cell_data)
                        # return
                    # elif dtype == "BLOB":
                        # print(cell_data, type(cell_data))
                        # return
                    elif target_dtype == "decimal":
                        converted_data = Decimal128(cell_data)
                    elif target_dtype == "object":
                        if type(cell_data) is str:
                            converted_data = cell_data
                        else:
                            converted_data = tuple(cell_data)
                    else:
                        converted_data = cell_data
                    data[col_fetch_seq[i]] = converted_data
            rows.append(data)
        # Parallel end here

        # assign to obj
        # store to mongodb
        # print("Start migrating table ", table)
        return rows
    # endregion

    # region Schema crawler methods
    def _get_schema_collection_tables(self) -> List:
        schema_collection = self._get_schema_collection()
        return list(filter(lambda record: isinstance(record, dict), schema_collection['catalog']['tables']))

    def _get_schema_collection_columns(self) -> List:
        schema_collection = self._get_schema_collection()
        return list(filter(lambda record: isinstance(record, dict), schema_collection['all-table-columns']))

    def _get_schema_collection(self) -> Any:
        if not self._does_collection_exist(MONGO_SCHEMA_COLLECTION):
            logger.error(f'error code: {MONGO_COLLECTION_NOT_FOUND}')
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

    def drop_db(self):
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
    # Override
    def set_db(self, db: str):
        self._db = db

    def _get_db_connection(self) -> Any:
        if not self._db:
            logger.error(f'error code: {MONGO_UNSPECIFIED_DATABASE_ERROR}')
            raise UnspecifiedDatabaseException('Set database first')
        conn = self._get_open_connection()
        return conn[self._db]

    # Override
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
