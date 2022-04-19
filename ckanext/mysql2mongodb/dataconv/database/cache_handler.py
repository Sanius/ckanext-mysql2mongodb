from typing import List, Union
import numpy as np

from redis import Redis, ConnectionPool

from ckanext.mysql2mongodb.dataconv.database.singleton import SingletonMetaCls
from ckanext.mysql2mongodb.settings import REDIS_HOST, REDIS_PORT, REDIS_DATABASE


class CacheHandler(metaclass=SingletonMetaCls):
    def __init__(self):
        self._host = REDIS_HOST
        self._port = REDIS_PORT
        self._db = REDIS_DATABASE
        self._pool = ConnectionPool(host=self._host, port=self._port, db=self._db)

    def append_list(self, key: str, value: Union[List, np.array]):
        """
        Push and pop from left to right
        """
        if not key or not value:
            return
        redis_client = self._get_db_connection()
        for i in value:
            redis_client.lpush(key, i)

    def get_list(self, key: str) -> List:
        if not key:
            return []
        redis_client = self._get_db_connection()
        return redis_client.lrange(key, 0, -1)

    def get_list_length(self, key: str) -> int:
        redis_client = self._get_db_connection()
        return redis_client.llen(key)

    def delete_entity(self, key: str):
        if not key:
            return
        redis_client = self._get_db_connection()
        redis_client.delete(key)

    def _get_db_connection(self) -> Redis:
        return Redis(decode_responses=True, charset='utf-8', connection_pool=self._pool)
