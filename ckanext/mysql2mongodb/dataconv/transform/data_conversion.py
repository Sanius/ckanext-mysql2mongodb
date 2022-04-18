import logging
from datetime import datetime
from typing import List, Dict, Tuple

from bson import Decimal128
from ckanext.mysql2mongodb.dataconv.constant.error_codes import MONGO_CONVERT_DATA_ERROR

from ckanext.mysql2mongodb.dataconv.constant.consts import MYSQL_MONGO_MAP, MONGO_DECIMAL_DATATYPE, \
    MONGO_OBJECT_DATATYPE, MYSQL_BINARY_DATATYPE, MYSQL_STRING_DATATYPE, MYSQL_DATE_DATATYPE, MYSQL_TIME_DATATYPE

logger = logging.getLogger(__name__)


def convert_mysql_to_mongodb(fetched_data: List[Tuple], column_datatype_list: List) -> List:
    try:
        return [_map_mysql_mongodb(mysql_record, column_datatype_list) for mysql_record in fetched_data]
    except Exception as ex:
        logger.error(f'error code: {MONGO_CONVERT_DATA_ERROR}')
        raise ex


def _map_mysql_mongodb(mysql_record: Tuple, mysql_columns_datatypes: List) -> Dict:
    mongo_record = {}
    for idx, column in enumerate(mysql_columns_datatypes):
        column_name = column['column_name']
        mysql_datatype = column['column_datatype']
        mongo_datatype = MYSQL_MONGO_MAP.get(mysql_datatype)
        cell_data = mysql_record[idx]
        if cell_data is None:
            continue
        if mysql_datatype in MYSQL_BINARY_DATATYPE:
            converted_data = bytes(cell_data)
        elif mysql_datatype in MYSQL_STRING_DATATYPE:
            converted_data = str(cell_data)
        elif mysql_datatype == MYSQL_DATE_DATATYPE:
            converted_data = datetime(cell_data.year, cell_data.month, cell_data.day)
        elif mongo_datatype == MONGO_DECIMAL_DATATYPE:
            converted_data = Decimal128(cell_data)
        elif mongo_datatype == MONGO_OBJECT_DATATYPE and not isinstance(cell_data, (str, dict)):
            converted_data = tuple(cell_data)
        elif mysql_datatype == MYSQL_TIME_DATATYPE:
            converted_data = '{:0>8}'.format(str(cell_data))
        else:
            converted_data = cell_data
        mongo_record[column_name] = converted_data
    return mongo_record
