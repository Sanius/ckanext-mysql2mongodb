import http
import logging
import os
from shutil import rmtree

import requests

from ckanext.mysql2mongodb.dataconv.exceptions import TempDirNotCreatedError, UnavailableResourceError, \
    UploadResourceError

from ckanext.mysql2mongodb.dataconv.settings import CKAN_API_KEY, CKAN_PROTOCOL, CKAN_HOST, CKAN_PORT

from ckanext.mysql2mongodb.dataconv.constant.consts import LOCAL_CKAN_DOWNLOAD_DIR, LOCAL_SCHEMA_CRAWLER_CACHE_DIR, \
    LOCAL_DATACONV_CACHE, LOCAL_MONGO_DUMP_CACHE_DIR, GZIP_FILE_EXTENSION

from ckanext.mysql2mongodb.dataconv.constant.error_codes import CREATE_TEMP_DIR_ERROR, \
    DOWNLOAD_CKAN_RESOURCE_ERROR, UPLOAD_RESOURCE_TO_CKAN_ERROR

logger = logging.getLogger(__name__)


# region Task procedures
def get_ckan_download_cache_path(resource_id: str) -> str:
    current_location = _get_current_location_absolute_path()
    return f'{current_location}/{LOCAL_CKAN_DOWNLOAD_DIR}/{resource_id}'


def get_mongo_dump_cache_path(resource_id: str) -> str:
    current_location = _get_current_location_absolute_path()
    return f'{current_location}/{LOCAL_MONGO_DUMP_CACHE_DIR}/{resource_id}'


def upload_to_ckan_mongo_dump_data(resource_id: str, sql_file_name: str, package_id: str):
    try:
        current_location = _get_current_location_absolute_path()
        file_name = f'{sql_file_name.split(".")[0]}.{GZIP_FILE_EXTENSION}'
        file_path = f'{current_location}/{LOCAL_MONGO_DUMP_CACHE_DIR}/{resource_id}/{file_name}'
        response = requests.post(f'{CKAN_PROTOCOL}://{CKAN_HOST}:{CKAN_PORT}/api/action/resource_create',
                                 data={'package_id': package_id,
                                       'name': file_name},
                                 headers={
                                     'X-CKAN-API-Key': CKAN_API_KEY},
                                 files={'upload': open(file_path, 'rb')})
        if response.status_code != http.HTTPStatus.OK:
            raise UploadResourceError('Cannot upload to ckan')
        logger.info(f'Upload resource {file_name} to ckan successfully!')
    except Exception as ex:
        logger.error(f'error code: {UPLOAD_RESOURCE_TO_CKAN_ERROR}')
        raise ex


def download_from_ckan_mysql_file(sql_file_url: str, resource_id: str, sql_file_name: str):
    current_location = _get_current_location_absolute_path()
    download_path = f'{current_location}/{LOCAL_CKAN_DOWNLOAD_DIR}/{resource_id}'
    try:
        _create_temp_dir(download_path)
        # region Download sql file
        # subprocess.run([
        #     f'curl -H \'X-CKAN-API-Key: {CKAN_API_KEY}\' -o {download_path}/{sql_file_name} {sql_file_url}'
        # ], shell=True, check=True)
        response = requests.get(sql_file_url, headers={'X-CKAN-API-Key': CKAN_API_KEY})
        if response.status_code != http.HTTPStatus.OK:
            logger.error(f'error code: {DOWNLOAD_CKAN_RESOURCE_ERROR}')
            raise UnavailableResourceError('Cannot download from ckan')
        open(f'{download_path}/{sql_file_name}', 'wb').write(response.content)
        # endregion
        logger.info('Download ckan resource successfully')
    except Exception as ex:
        logger.error(f'error code: {DOWNLOAD_CKAN_RESOURCE_ERROR}')
        raise ex
# endregion


def create_schema_crawler_cache_dir(extra_dir: str) -> str:
    current_location = _get_current_location_absolute_path()
    schema_crawler_cache_path = f'{current_location}/{LOCAL_SCHEMA_CRAWLER_CACHE_DIR}/{extra_dir}'
    _create_temp_dir(schema_crawler_cache_path)
    return schema_crawler_cache_path


def create_mongo_dump_cache_dir(extra_dir: str) -> str:
    current_location = _get_current_location_absolute_path()
    mongo_dump_cache_path = f'{current_location}/{LOCAL_MONGO_DUMP_CACHE_DIR}/{extra_dir}'
    _create_temp_dir(mongo_dump_cache_path)
    return mongo_dump_cache_path


def clear_dataconv_cache():
    current_location = _get_current_location_absolute_path()
    dataconv_cache_path = f'{current_location}/{LOCAL_DATACONV_CACHE}'
    if os.path.exists(dataconv_cache_path) and os.path.isdir(dataconv_cache_path):
        rmtree(dataconv_cache_path)


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
