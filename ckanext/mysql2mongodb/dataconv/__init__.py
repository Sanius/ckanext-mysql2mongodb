import logging

import pprint
import json

from airflow.api.client.local_client import Client

from datetime import datetime

from ckanext.mysql2mongodb.dataconv.constant.error_codes import CONVERT_DATA_ERROR

logger = logging.getLogger(__name__)


def convert_data(resource_id, sql_file_name, sql_file_url, package_id):
    try:
        conf = {
            'resource_id': resource_id,
            'sql_file_name': sql_file_name,
            'sql_file_url': sql_file_url,
            'package_id': package_id
        }
        c = Client(None, None)
        timestamp = datetime.timestamp(datetime.now())
        c.trigger_dag(dag_id='data_conversion_flow',
                      run_id='data_conversion_flow' + str(timestamp),
                      conf=json.dumps(conf))
        return True
    except Exception as e:
        logger.error(f'error code: {CONVERT_DATA_ERROR}')
        logger.error(pprint.pformat(e))
