from ckanext.mysql2mongodb.dataconv.database.AbstractDatabaseHandler import AbstractDatabaseHandler

from ckanext.mysql2mongodb.dataconv.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_USERNAME, MYSQL_PASSWORD


class MySQLHandler(AbstractDatabaseHandler):
    def __init__(self):
        self._host = MYSQL_HOST
        self._port = MYSQL_PORT
        self._username = MYSQL_USERNAME
        self._password = MYSQL_PASSWORD

    # Override
    def restore(self):
        pass

    # Override
    def backup(self):
        pass
