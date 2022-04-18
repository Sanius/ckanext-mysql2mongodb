from typing import Any
import datetime


def transform_mysql_data_for_coreset_algorithm(pandas_cell: Any) -> float:
    if isinstance(pandas_cell, (bytes, str, list, set, dict)):
        return len(pandas_cell)
    if isinstance(pandas_cell, float):
        return pandas_cell
    if isinstance(pandas_cell, datetime.datetime):
        return pandas_cell.timestamp()
    return 0
