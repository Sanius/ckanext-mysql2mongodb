import os

CKAN_API_KEY = os.environ.get('X_CKAN_API_KEY')

MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_PORT = os.environ.get('MYSQL_PORT')
MYSQL_USERNAME = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_PORT = os.environ.get('MONGO_PORT')
MONGO_USERNAME = os.environ.get('MONGO_USER')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD')
MONGO_DB = os.environ.get('MONGO_DB')

MYSQL_ENV_VAR_PATH = '' if not os.environ.get('MYSQL_HOME') else f'{os.environ.get("MYSQL_HOME")}/bin/'
SCHEMA_CRAWLER_ENV_VAR_PATH = '' if not os.environ.get('MYSQL_HOME') \
    else f'{os.environ.get("SCHEMA_CRAWLER_HOME")}/bin/'
