from typing import Any

from ckanext.mysql2mongodb.dataconv.settings import MONGO_HOST, MONGO_PORT, MONGO_USERNAME, MONGO_PASSWORD

from ckanext.mysql2mongodb.dataconv.database.abstract_database_handler import AbstractDatabaseHandler


class SchemaHandler(AbstractDatabaseHandler):
    # Override
    def __init__(self):
        super().__init__()
        self._host = MONGO_HOST
        self._port = MONGO_PORT
        self._username = MONGO_USERNAME
        self._password = MONGO_PASSWORD

    # Override
    def _set_db(self, db: str): raise NotImplementedError

    # Override
    def _get_open_connection(self) -> Any: raise NotImplementedError
