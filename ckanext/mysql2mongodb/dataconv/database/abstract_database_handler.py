from abc import abstractmethod, ABC
from typing import Any


class AbstractDatabaseHandler(ABC):
    def __init__(self):
        self._host = None
        self._port = None
        self._username = None
        self._password = None
        self._db = None

    @abstractmethod
    def _set_db(self, db: str): raise NotImplementedError

    @abstractmethod
    def _get_open_connection(self) -> Any: raise NotImplementedError
