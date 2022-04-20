from typing import List, Union
import numpy as np
import pandas as pd
import pickle

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

    def get_list(self, key: str) -> np.array:
        if not key:
            return np.array([])
        redis_client = self._get_db_connection()
        return np.array(redis_client.lrange(key, 0, -1))

    def get_list_length(self, key: str) -> int:
        redis_client = self._get_db_connection()
        return redis_client.llen(key)

    def delete_entity(self, key: str):
        if not key:
            return
        redis_client = self._get_db_connection()
        redis_client.delete(key)

    def clear_cache(self):
        redis_client = self._get_db_connection()
        for key in redis_client.keys():
            redis_client.delete(key)

    def store_dataframe(self, key: str, value: pd.DataFrame):
        if not key or value.empty:
            return
        redis_client = Redis(connection_pool=self._pool, decode_responses=False)
        redis_client.set(key, pickle.dumps(value))

    def get_dataframe(self, key: str) -> pd.DataFrame:
        if not key:
            return pd.DataFrame()
        redis_client = Redis(connection_pool=self._pool, decode_responses=False)
        return pickle.loads(redis_client.get(key))

    def is_dataframe_saved(self, key: str) -> bool:
        redis_client = self._get_db_connection()
        return False if not key else redis_client.exists(key) == 1

    def _get_db_connection(self) -> Redis:
        return Redis(decode_responses=True, charset='utf-8', connection_pool=self._pool)
