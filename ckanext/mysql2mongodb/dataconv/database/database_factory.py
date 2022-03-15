from ckanext.mysql2mongodb.dataconv.database.mongo_handler import MongoHandler

from ckanext.mysql2mongodb.dataconv.database.mysql_handler import MySQLHandler

from ckanext.mysql2mongodb.dataconv.exceptions import UnsupportedDatabaseException

from ckanext.mysql2mongodb.dataconv.constant.consts import SUPPORTED_DATABASES, MYSQL, MONGO


def produce_database(db_type: str):
    if not db_type or db_type.lower() not in SUPPORTED_DATABASES:
        raise UnsupportedDatabaseException('Unsupported database')
    if db_type.lower() == MYSQL:
        return MySQLHandler()
    if db_type.lower() == MONGO:
        return MongoHandler()
