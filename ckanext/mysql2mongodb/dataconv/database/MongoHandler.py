from ckanext.mysql2mongodb.dataconv.database.AbstractDatabaseHandler import AbstractDatabaseHandler

from ckanext.mysql2mongodb.dataconv.settings import MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD


class MongoHandler(AbstractDatabaseHandler):
    def __init__(self):
        self._host = MONGO_HOST
        self._port = MONGO_PORT
        self._username = MONGO_USERNAME
        self._password = MONGO_PASSWORD

    # Override
    def restore(self):
        pass

    # Override
    def backup(self):
        pass
