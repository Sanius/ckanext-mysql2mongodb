import urllib, json, re, os, requests
from pprint import pprint
from dotenv import load_dotenv

from ckanext.mysql2mongodb.data_conv.schema_conversion import SchemaConversion
from ckanext.mysql2mongodb.data_conv.database_connection import ConvInitOption, ConvOutputOption
from ckanext.mysql2mongodb.data_conv.data_conversion import DataConversion
from ckanext.mysql2mongodb.data_conv.utilities import open_connection_mysql

def read_package_config(file_url = "package_config.txt"):
    package_conf = {}
    load_dotenv()
    # with open(file_url, "r") as f:
    #     lines = f.readlines()

    # for line in lines:
    #     look_for_conf = re.search("^package_id", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         package_conf["package_id"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^X-CKAN-API-Key", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         package_conf["X-CKAN-API-Key"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    package_conf["package_id"] = os.getenv('PACKAGE_ID')
    package_conf["X-CKAN-API-Key"] = os.getenv('X_CKAN_API_KEY')

    return package_conf

def read_database_config():
    db_conf = {}
    load_dotenv()
    # file_url = "database_config.txt"
    # with open(file_url, "r") as f:
    #     lines = f.readlines()

    # for line in lines:
    #     look_for_conf = re.search("^mysql_host", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mysql_host"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mysql_port", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mysql_port"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mysql_password", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mysql_password"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mysql_username", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mysql_username"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mongodb_host", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mongodb_host"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mongodb_username", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mongodb_username"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mongodb_port", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mongodb_port"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    #     look_for_conf = re.search("^mongodb_password", line.strip(), re.IGNORECASE)
    #     if look_for_conf is not None:
    #         db_conf["mongodb_password"] = re.split(r'[\s]+=[\s]+', line.strip())[1][1:-1]

    db_conf["mysql_host"] = os.getenv('MYSQL_HOST')
    db_conf["mysql_port"] = os.getenv('MYSQL_PORT')
    db_conf["mysql_username"] = os.getenv('MYSQL_USERNAME')
    db_conf["mysql_password"] = os.getenv('MYSQL_PASSWORD')

    db_conf["mongodb_host"] = os.getenv('MONGODB_HOST')
    db_conf["mongodb_port"] = os.getenv('MONGODB_PORT')
    db_conf["mongodb_username"] = os.getenv('MONGODB_USERNAME')
    db_conf["mongodb_password"] = os.getenv('MONGODB_PASSWORD')

    return db_conf