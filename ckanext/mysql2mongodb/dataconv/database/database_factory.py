from ckanext.mysql2mongodb.dataconv.database.MongoHandler import MongoHandler

from ckanext.mysql2mongodb.dataconv.database.MySQLHandler import MySQLHandler

from ckanext.mysql2mongodb.dataconv.exceptions import UnsupportedDatabaseException

from ckanext.mysql2mongodb.dataconv.constant.consts import SUPPORTED_DATABASES


def produce_database(db_type: str):
    if not db_type or db_type.lower() not in SUPPORTED_DATABASES:
        raise UnsupportedDatabaseException('Unsupported database')
    if db_type.lower() == 'mysql':
        return MySQLHandler()
    if db_type.lower() == 'mongo':
        return MongoHandler()
