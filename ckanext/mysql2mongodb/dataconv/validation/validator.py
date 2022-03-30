from ckanext.mysql2mongodb.dataconv.database.mysql_handler import MySQLHandler

from ckanext.mysql2mongodb.dataconv.database.mongo_handler import MongoHandler


class Validator:
    def __init__(self):
        self._mongo_handler = MongoHandler()
        self._mysql_handler = MySQLHandler()
