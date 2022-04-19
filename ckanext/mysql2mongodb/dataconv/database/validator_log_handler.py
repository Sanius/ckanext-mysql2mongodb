import logging
import uuid
from typing import Dict, List

import pandas as pd
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker

from ckanext.mysql2mongodb.dataconv.constant.consts import CSV_FILE_EXTENSION, LOCAL_VALIDATOR_LOG_REPORT_DIR, \
    XLSX_FILE_EXTENSION
from ckanext.mysql2mongodb.dataconv.constant.error_codes import VALIDATOR_LOG_INSERT_ERROR, VALIDATOR_LOG_EXPORT_ERROR
from ckanext.mysql2mongodb.dataconv.database.singleton import SingletonMetaCls
from ckanext.mysql2mongodb.dataconv.exceptions import LogDataInsufficiencyException
from ckanext.mysql2mongodb.dataconv.file_system import file_system_handler
from ckanext.mysql2mongodb.models import ValidatorLogger
from ckanext.mysql2mongodb.settings import POSTGRESQL_LOG_HOST, POSTGRESQL_LOG_USER, POSTGRESQL_LOG_PASSWORD, \
    POSTGRESQL_LOG_PORT, POSTGRESQL_LOG_DATABASE

logger = logging.Logger(__name__)


class ValidatorLogHandler(metaclass=SingletonMetaCls):
    def __init__(self):
        self._engine = create_engine(
            'postgresql+psycopg2://{postgresql_user}:{postgresql_password}@{postgresql_host}:{postgresql_port}/{postgresql_database}'
                .format(postgresql_user=POSTGRESQL_LOG_USER,
                        postgresql_password=POSTGRESQL_LOG_PASSWORD,
                        postgresql_host=POSTGRESQL_LOG_HOST,
                        postgresql_port=POSTGRESQL_LOG_PORT,
                        postgresql_database=POSTGRESQL_LOG_DATABASE,
                        )
        )
        self._db_session = sessionmaker(bind=self._engine)
        self._log_table = ValidatorLogger

    def __del__(self):
        sessionmaker.close_all()

    def write_log(self, resource_id: str, package_id: str, database: str, table: str, description: str):
        try:
            if not (resource_id and package_id and database and table):
                raise LogDataInsufficiencyException('Incorrect data')
            new_log = ValidatorLogger(
                log_id=str(uuid.uuid4()),
                resource_id=resource_id,
                package_id=package_id,
                database=database,
                table=table,
                description=description
            )
            session = self._db_session()
            session.add(new_log)
            session.commit()
            logger.info(f'new log added, info {new_log}')
        except Exception as ex:
            logger.error(f'error code: {VALIDATOR_LOG_INSERT_ERROR}')
            raise ex

    def export_validator_log_csv(self, resource_id: str, package_id: str):
        try:
            query = {
                'package_id': [package_id],
                'resource_id': [resource_id]
            }
            df = self._select_by_packageid_resourceid(query)
            file_system_handler.create_dataconv_cache_dir(LOCAL_VALIDATOR_LOG_REPORT_DIR, resource_id)
            file_path = f'{file_system_handler.get_dataconv_cache_dir_path(LOCAL_VALIDATOR_LOG_REPORT_DIR, resource_id)}/{package_id}.{CSV_FILE_EXTENSION} '
            df.to_csv(file_path)
            logger.info('Validator report exported')
        except Exception as ex:
            logger.error(f'error code: {VALIDATOR_LOG_EXPORT_ERROR}')
            raise ex

    def export_validator_log_xlsx(self, resource_id: str, package_id: str):
        try:
            query = {
                'package_id': [package_id],
                'resource_id': [resource_id]
            }
            df = self._select_by_packageid_resourceid(query)
            file_system_handler.create_dataconv_cache_dir(LOCAL_VALIDATOR_LOG_REPORT_DIR, resource_id)
            file_path = f'{file_system_handler.get_dataconv_cache_dir_path(LOCAL_VALIDATOR_LOG_REPORT_DIR, resource_id)}/{package_id}.{XLSX_FILE_EXTENSION}'
            df.to_excel(file_path)
            logger.info('Validator report exported')
        except Exception as ex:
            logger.error(f'error code: {VALIDATOR_LOG_EXPORT_ERROR}')
            raise ex

    def _select_by_packageid_resourceid(self, query: Dict[str, List]) -> pd.DataFrame:
        """
        input:
        {
            resource_id: [val1, val2, val3],
            package_id: [val1, val2, val3],
        }
        """
        session = self._db_session()
        query = session.query(self._log_table).filter(
            and_(
                self._log_table.package_id.in_(query['package_id']),
                self._log_table.resource_id.in_(query['resource_id'])
            )
        )
        return pd.read_sql(query.statement, con=session.bind, index_col=['log_id'])
