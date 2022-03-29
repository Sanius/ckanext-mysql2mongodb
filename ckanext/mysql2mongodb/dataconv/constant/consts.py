# External file extension
from typing import Tuple, Dict

SQL_FILE_EXTENSION = 'sql'
JSON_FILE_EXTENSION = 'json'
GZIP_FILE_EXTENSION = 'gz'
# Local cache
LOCAL_DATACONV_CACHE = '.dataconv_cache'
LOCAL_CKAN_DOWNLOAD_DIR = f'{LOCAL_DATACONV_CACHE}/ckan_downloads'
LOCAL_SCHEMA_CRAWLER_CACHE_DIR = f'{LOCAL_DATACONV_CACHE}/schema_crawler_cache'
LOCALMONGO_DUMP_CACHE_DIR = f'{LOCAL_DATACONV_CACHE}/mongodump'
# DATABASE
MONGO = 'mongo'
MYSQL = 'mysql'
# Command line
SCHEMA_CRAWLER = 'schemacrawler.sh'
MONGO_DUMP = 'mongodump'
# Mongo
MONGO_SCHEMA_COLLECTION = '_schema'

# region Mysql mongo mapping
MONGO_INTEGER_DATATYPE = 'integer'
MONGO_DECIMAL_DATATYPE = 'decimal'
MONGO_DOUBLE_DATATYPE = 'double'
MONGO_BOOLEAN_DATATYPE = 'boolean'
MONGO_DATE_DATATYPE = 'date'
MONGO_TIMESTAMP_DATATYPE = 'timestamp'
MONGO_BINARY_DATATYPE = 'binary'
MONGO_BLOB_DATATYPE = 'blob'
MONGO_STRING_DATATYPE = 'string'
MONGO_OBJECT_DATATYPE = 'object'
MONGO_SINGLE_GEOMETRY_DATATYPE = 'single-geometry'
MONGO_MULTIPLE_GEOMETRY_DATATYPE = 'multiple-geometry'

MYSQL_INTEGER_DATATYPE: Tuple = ('TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'INTEGER', 'BIGINT')
MYSQL_DECIMAL_DATATYPE: Tuple = ('DECIMAL', 'DEC', 'FIXED')
MYSQL_DOUBLE_DATATYPE: Tuple = ('FLOAT', 'DOUBLE', 'REAL')
MYSQL_BOOLEAN_DATATYPE: Tuple = ('BOOL', 'BOOLEAN')
MYSQL_DATE_DATATYPE: Tuple = ('DATE', 'YEAR')
MYSQL_TIMESTAMP_DATATYPE: Tuple = ('DATETIME', 'TIMESTAMP', 'TIME')
MYSQL_BINARY_DATATYPE: Tuple = ('BIT', 'BINARY', 'VARBINARY')
MYSQL_BLOB_DATATYPE: Tuple = ('TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB')
MYSQL_STRING_DATATYPE: Tuple = ('CHARACTER', 'CHARSET', 'ASCII', 'UNICODE', 'CHAR', 'VARCHAR', 'TINYTEXT', 'TEXT',
                                'MEDIUMTEXT', 'LONGTEXT')
MYSQL_OBJECT_DATATYPE: Tuple = ('ENUM', 'SET', 'JSON')
MYSQL_SINGLE_GEOMETRY_DATATYPE: Tuple = ('GEOMETRY', 'POINT', 'LINESTRING', 'POLYGON')
MYSQL_MULTIPLE_GEOMETRY_DATATYPE: Tuple = ('MULTIPOINT', 'MULTILINESTRING', 'MULTIPOLYGON', 'GEOMETRYCOLLECTION')

MYSQL_MONGO_MAP: Dict[str, str] = {
    **{mysql_datatype: MONGO_INTEGER_DATATYPE for mysql_datatype in MYSQL_INTEGER_DATATYPE},
    **{mysql_datatype: MONGO_DECIMAL_DATATYPE for mysql_datatype in MYSQL_DECIMAL_DATATYPE},
    **{mysql_datatype: MONGO_DOUBLE_DATATYPE for mysql_datatype in MYSQL_DOUBLE_DATATYPE},
    **{mysql_datatype: MONGO_BOOLEAN_DATATYPE for mysql_datatype in MYSQL_BOOLEAN_DATATYPE},
    **{mysql_datatype: MONGO_DATE_DATATYPE for mysql_datatype in MYSQL_DATE_DATATYPE},
    **{mysql_datatype: MONGO_TIMESTAMP_DATATYPE for mysql_datatype in MYSQL_TIMESTAMP_DATATYPE},
    **{mysql_datatype: MONGO_BINARY_DATATYPE for mysql_datatype in MYSQL_BINARY_DATATYPE},
    **{mysql_datatype: MONGO_BLOB_DATATYPE for mysql_datatype in MYSQL_BLOB_DATATYPE},
    **{mysql_datatype: MONGO_STRING_DATATYPE for mysql_datatype in MYSQL_STRING_DATATYPE},
    **{mysql_datatype: MONGO_OBJECT_DATATYPE for mysql_datatype in MYSQL_OBJECT_DATATYPE},
    **{mysql_datatype: MONGO_SINGLE_GEOMETRY_DATATYPE for mysql_datatype in MYSQL_SINGLE_GEOMETRY_DATATYPE},
    **{mysql_datatype: MONGO_MULTIPLE_GEOMETRY_DATATYPE for mysql_datatype in MYSQL_MULTIPLE_GEOMETRY_DATATYPE},
}
# endregion