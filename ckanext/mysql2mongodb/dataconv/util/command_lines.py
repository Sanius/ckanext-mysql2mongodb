import logging
import os
import subprocess

from ckanext.mysql2mongodb.dataconv.exceptions import TempDirNotCreatedError

from ckanext.mysql2mongodb.dataconv.settings import CKAN_API_KEY

from ckanext.mysql2mongodb.dataconv.constant.consts import LOCAL_CKAN_DOWNLOAD_DIR

from ckanext.mysql2mongodb.dataconv.constant.error_codes import CREATE_TEMP_DIR_ERROR, \
    DOWNLOAD_CKAN_RESOURCE_ERROR

logger = logging.getLogger(__name__)


def download_from_ckan_mysql_file(sql_file_url: str, resource_id: str, sql_file_name: str):
    current_location = _get_current_location_absolute_path()
    download_path = f'{current_location}/{LOCAL_CKAN_DOWNLOAD_DIR}/{resource_id}'
    try:
        _create_temp_dir(download_path)
        subprocess.run([
            f'curl -H \'X-CKAN-API-Key: {CKAN_API_KEY}\' -o {download_path}/{sql_file_name} {sql_file_url}'
        ], shell=True, check=True)
        logger.info('Download ckan resource successfully')
    except Exception as ex:
        logger.error(f'error code: {DOWNLOAD_CKAN_RESOURCE_ERROR}')
        raise ex


def _create_temp_dir(dir_path: str):
    if not dir_path:
        logger.error(f'error code: {CREATE_TEMP_DIR_ERROR}')
        raise TempDirNotCreatedError('Cannot create temporary directory')
    try:
        os.makedirs(f'{dir_path}', exist_ok=True)
        logger.debug('Create temporary directory successfully')
    except Exception as ex:
        logger.error(f'error code: {CREATE_TEMP_DIR_ERROR}')
        raise ex


def _get_current_location_absolute_path() -> str:
    return os.path.abspath('.')
