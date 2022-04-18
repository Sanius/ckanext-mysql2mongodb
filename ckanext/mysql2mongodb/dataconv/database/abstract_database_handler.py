from abc import abstractmethod, ABC
from typing import Any


class AbstractDatabaseHandler(ABC):
    def __init__(self):
        self._host = None
        self._port = None
        self._username = None
        self._password = None

    @abstractmethod
    def _get_open_connection(self) -> Any: raise NotImplementedError

    @abstractmethod
    def _does_db_exist(self, db_name: str) -> bool: raise NotImplementedError
