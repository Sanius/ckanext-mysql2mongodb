from redis import Redis

from ckanext.mysql2mongodb.settings import REDIS_HOST, REDIS_PORT, REDIS_DATABASE


class CacheHandler:
    def __init__(self):
        self._host = REDIS_HOST
        self._port = REDIS_PORT
        self._db = REDIS_DATABASE

    # Override
    def _get_db_connection(self) -> Redis:
        return Redis(host=self._host, port=self._port, db=self._db)
