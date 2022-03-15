from abc import abstractmethod, ABC


class AbstractDatabaseHandler(ABC):
    def __init__(self):
        self._host = None
        self._port = None
        self._username = None
        self._password = None
        self._db = None

    @abstractmethod
    def set_db(self, db: str): raise NotImplementedError

    @abstractmethod
    def restore_from_ckan(self, resource_id: str, file_name: str): raise NotImplementedError

    @abstractmethod
    def backup(self): raise NotImplementedError
