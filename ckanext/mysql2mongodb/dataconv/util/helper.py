from typing import Union, Dict, List

import pandas as pd


def from_pandas_index_to_dict(pandas_index: Union[pd.Index, pd.MultiIndex]) -> Dict[str, List]:
    """
    output:
    {
        index_1.name: [val1, val2, val3],
        index_2.name: [val1, val2, val3],
        index_3.name: [val1, val2, val3],
    }
    """
    if type(pandas_index) == pd.MultiIndex:
        return {index_name: [index_val[index_idx] for index_val in pandas_index]
                for index_idx, index_name in enumerate(pandas_index.names)}
    return {pandas_index.name: list(pandas_index)}


def from_pandas_index_dict_to_mongodb_query(index_dict: Dict[str, list]) -> Dict[str, Dict[str, List]]:
    """
    input:
    {
        index_1.name: [val1, val2, val3],
        index_2.name: [val1, val2, val3],
        index_3.name: [val1, val2, val3],
    }
    output:
    {
        index_1.name: {'$in': [val1, val2, val3]},
        index_2.name: {'$in': [val1, val2, val3]},
        index_3.name: {'$in': [val1, val2, val3]},
    }
    """
    return {index_name: {'$in': index_value} for index_name, index_value in index_dict.items()}
