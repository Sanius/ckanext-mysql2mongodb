from typing import Any

from ckanext.mysql2mongodb.dataconv.settings import POSTGRESQL_LOG_HOST, POSTGRESQL_LOG_USER, POSTGRESQL_LOG_PASSWORD, \
    POSTGRESQL_LOG_PORT

from ckanext.mysql2mongodb.dataconv.database.abstract_database_handler import AbstractDatabaseHandler


class SchemaHandler(AbstractDatabaseHandler):
    # Override
    def __init__(self):
        super().__init__()
        self._host = POSTGRESQL_LOG_HOST
        self._port = POSTGRESQL_LOG_PORT
        self._username = POSTGRESQL_LOG_USER
        self._password = POSTGRESQL_LOG_PASSWORD

    # Override
    def _set_db(self, db: str): raise NotImplementedError

    # Override
    def _get_open_connection(self) -> Any: raise NotImplementedError
