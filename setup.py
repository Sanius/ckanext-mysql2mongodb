from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='ckanext-mysql2mongodb',
    version='0.0.1',
    description='''A data conversion data from MySQL to MongoDB project from BKU''',
    url='https://github.com/sanius/ckanext-mysql2mongodb',
    keywords='''CKAN MySQL MongoDB conversion BKU''',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    namespace_packages=['ckanext'],
    include_package_data=True,
    entry_points='''
        [ckan.plugins]
        ckanext_mysql2mongodb=ckanext.mysql2mongodb.plugin:Mysql2MongodbPlugin

        [babel.extractors]
        ckan = ckan.lib.extract:extract_ckan
    ''',
    message_extractors={
        'ckanext': [
            ('**.py', 'python', None),
            ('**.js', 'javascript', None),
            ('**/templates/**.html', 'ckan', None),
        ],
    }
)
