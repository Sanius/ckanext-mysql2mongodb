from abc import ABC, abstractmethod
from typing import Any, Dict
from multiprocessing import RLock


class AbstractDatabaseHandler(ABC):
    _instances: Dict = {}
    _lock: RLock = RLock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__new__(cls, *args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]

    def __init__(self):
        self._host = None
        self._port = None
        self._username = None
        self._password = None

    @abstractmethod
    def _get_open_connection(self) -> Any: raise NotImplementedError

    @abstractmethod
    def _does_db_exist(self, db_name: str) -> bool: raise NotImplementedError
