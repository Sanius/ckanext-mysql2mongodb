from abc import abstractmethod, ABC


class AbstractDatabaseHandler(ABC):
    @abstractmethod
    def restore(self): raise NotImplementedError

    @abstractmethod
    def backup(self): raise NotImplementedError
