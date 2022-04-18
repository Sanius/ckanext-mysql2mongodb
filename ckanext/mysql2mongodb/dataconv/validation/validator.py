import pandas as pd
import numpy as np
import datetime
import json
import bson
from shapely import wkt

from ckanext.mysql2mongodb.dataconv.constant.consts import ROWS_NOT_MATCH, INCORRECT_VALUE, DICT, SET, GEOMETRY, \
    DECIMAL, DATE, NORMAL
from ckanext.mysql2mongodb.dataconv.database import MySQLHandler, MongoHandler
from ckanext.mysql2mongodb.dataconv.exceptions import ValidationFlowIncompleteError


def compare_total_rows(mysql_handler: MySQLHandler, mongo_handler: MongoHandler, db_name: str, table_name: str):
    if mysql_handler.count_table(db_name, table_name) != mongo_handler.count_table(db_name, table_name):
        raise ValidationFlowIncompleteError(ROWS_NOT_MATCH)


def find_false_indexes(mysql_df: pd.DataFrame, mongo_df: pd.DataFrame):
    validator_mat = np.array([], dtype='bool')
    for column_name in mysql_df:
        compare_arr = _validate_coreset_transformed_mysql_mongodb(mysql_df[column_name], mongo_df[column_name])
        validator_mat = np.hstack([validator_mat, compare_arr]) if validator_mat.size == 0 \
            else np.vstack([validator_mat, compare_arr])
    validator_arr = np.logical_and.reduce(validator_mat)
    false_indexes = np.array([], dtype='object')
    if not np.logical_and.reduce(validator_arr):
        for idx, val in enumerate(validator_arr):
            if not val:
                false_indexes = np.append(false_indexes, mysql_df.iloc[idx].name)
    return false_indexes
    # if false_indexes.size != 0:
    #     raise ValidationFlowIncompleteError(INCORRECT_VALUE(false_indexes.size))


def _validate_coreset_transformed_mysql_mongodb(mysql_col: pd.Series, mongo_col: pd.Series) -> np.array:
    def check_type() -> str:
        if isinstance(mysql_col.iloc[0], dict) and isinstance(mongo_col.iloc[0], str):
            return DICT
        if isinstance(mysql_col.iloc[0], set) and isinstance(mongo_col.iloc[0], list):
            return SET
        if isinstance(mysql_col.iloc[0], bytes) and isinstance(mongo_col.iloc[0], str):
            return GEOMETRY
        if isinstance(mongo_col.iloc[0], bson.Decimal128):
            return DECIMAL
        if isinstance(mysql_col.iloc[0], datetime.time) and isinstance(mongo_col.iloc[0], str):
            return DATE
        return NORMAL

    def validate_dict() -> np.array:
        mongo_data = mongo_col.apply(func=json.loads)
        return np.where(mysql_col == mongo_data, True, False)

    def validate_set() -> np.array:
        mongo_data = mongo_col.apply(func=set)
        return np.where(mysql_col == mongo_data, True, False)

    def validate_geometry() -> np.array:
        start_byte = 4
        mysql_data = mysql_col.apply(lambda x: x[start_byte:])
        mongo_data = mongo_col.apply(lambda x: wkt.loads(x).wkb)
        return np.where(mysql_data == mongo_data, True, False)

    def validate_decimal() -> np.array:
        mongo_data = mongo_col.astype(str).astype(float)
        return np.where(mysql_col == mongo_data, True, False)

    def validate_date() -> np.array:
        mysql_data = mysql_col.apply(lambda x: x.strftime('%H:%M:%S'))
        return np.where(mysql_data == mongo_col, True, False)

    def validate() -> np.array:
        return np.where(mysql_col == mongo_col, True, False)

    if (compare_type := check_type()) == SET:
        return validate_set()
    if compare_type == GEOMETRY:
        return validate_geometry()
    if compare_type == DECIMAL:
        return validate_decimal()
    if compare_type == DATE:
        return validate_date()
    if compare_type == DICT:
        return validate_dict()
    return validate()
