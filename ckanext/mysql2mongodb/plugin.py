import logging

import ckan.plugins as ckan_plugins
import ckan.plugins.toolkit as ckan_toolkit
from ckanext.mysql2mongodb.dataconv import convert_data

logger = logging.getLogger(__name__)


class Mysql2MongodbPlugin(ckan_plugins.SingletonPlugin):
    ckan_plugins.implements(ckan_plugins.IResourceController)

    def after_create(self, context, resource):
        ckan_toolkit.enqueue_job(convert_data, [
            resource['id'],
            resource['name'],
            resource['url'],
            resource['package_id']
        ])

    def before_create(self, context, resource):
        pass

    def before_update(self, context, current, resource):
        pass

    def after_update(self, context, resource):
        pass

    def before_delete(self, context, resource, resources):
        pass

    def after_delete(self, context, resources):
        pass

    def before_show(self, resource_dict):
        pass
