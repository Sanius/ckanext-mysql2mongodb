import os

CKAN_API_KEY = os.environ.get('X_CKAN_API_KEY')
CKAN_PROTOCOL = os.environ.get('CKAN_PROTOCOL', 'http')
CKAN_HOST = os.environ.get('CKAN_HOST', '127.0.0.1')
CKAN_PORT = os.environ.get('CKAN_PORT', 5000)

MYSQL_HOST = os.environ.get('MYSQL_HOST')
MYSQL_PORT = os.environ.get('MYSQL_PORT')
MYSQL_USERNAME = os.environ.get('MYSQL_USER')
MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')

MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_PORT = os.environ.get('MONGO_PORT')
MONGO_USERNAME = os.environ.get('MONGO_USER')
MONGO_PASSWORD = os.environ.get('MONGO_PASSWORD')

POSTGRESQL_LOG_HOST = os.environ.get('POSTGRESQL_LOG_HOST')
POSTGRESQL_LOG_USER = os.environ.get('POSTGRESQL_LOG_USER')
POSTGRESQL_LOG_PASSWORD = os.environ.get('POSTGRESQL_LOG_PASSWORD')
POSTGRESQL_LOG_PORT = os.environ.get('POSTGRESQL_LOG_PORT')

MYSQL_ENV_VAR_PATH = os.environ.get('MYSQL_BIN', '')
SCHEMA_CRAWLER_ENV_VAR_PATH = os.environ.get('SCHEMA_CRAWLER_BIN', '')
MONGO_TOOL_ENV_VAR_PATH = os.environ.get('MONGO_TOOL_BIN', '')
