import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import prepare as mysql2mongo_prepare
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import converse_schema as mysql2mongo_converse_schema
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import converse_data as mysql2mongo_converse_data
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import upload_converted_data as mysql2mongo_upload_data

logger = logging.getLogger(__name__)


def _task_prepare(**kwargs):
    file_info = {
        'sql_file_url': kwargs['dag_run'].conf.get('sql_file_url'),
        'resource_id': kwargs['dag_run'].conf.get('resource_id'),
        'sql_file_name': kwargs['dag_run'].conf.get('sql_file_name'),
    }
    mysql2mongo_prepare(**file_info)


def _task_converse_schema(**kwargs):
    mysql2mongo_converse_schema()


def _task_converse_data(**kwargs):
    mysql2mongo_converse_data()


def _task_upload_result(**kwargs):
    mysql2mongo_upload_data()


def pull_from_xcom(kwargs):
    resource_id = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='resource_id')
    sql_file_name = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='sql_file_name')
    sql_file_url = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='sql_file_url')
    db_conf = kwargs['ti'].xcom_pull(task_ids='taskPrepare', key='db_conf')
    schema_name = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='schema_name')
    mysql_host = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_host')
    mysql_username = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_username')
    mysql_password = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_password')
    mysql_port = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_port')
    mysql_dbname = kwargs['ti'].xcom_pull(
        task_ids='taskPrepare', key='mysql_dbname')
    return resource_id, sql_file_name, sql_file_url, db_conf, schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname


def push_to_xcom(kwargs, resource_id, sql_file_name, sql_file_url, db_conf, package_conf, CKAN_API_KEY, schema_name, mysql_host, mysql_username, mysql_password, mysql_port, mysql_dbname):
    kwargs['ti'].xcom_push(key='resource_id', value=resource_id)
    kwargs['ti'].xcom_push(key='sql_file_name', value=sql_file_name)
    kwargs['ti'].xcom_push(key='sql_file_url', value=sql_file_url)
    kwargs['ti'].xcom_push(key='db_conf', value=db_conf)
    kwargs['ti'].xcom_push(key='package_conf', value=package_conf)
    kwargs['ti'].xcom_push(key='CKAN_API_KEY', value=CKAN_API_KEY)
    kwargs['ti'].xcom_push(key='schema_name', value=schema_name)
    kwargs['ti'].xcom_push(key='mysql_host', value=mysql_host)
    kwargs['ti'].xcom_push(key='mysql_username', value=mysql_username)
    kwargs['ti'].xcom_push(key='mysql_password', value=mysql_password)
    kwargs['ti'].xcom_push(key='mysql_port', value=mysql_port)
    kwargs['ti'].xcom_push(key='mysql_dbname', value=mysql_dbname)


dag = DAG('data_conversion_flow',
          description='Data Conversion Flow',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 3, 4), catchup=False)

task1 = PythonOperator(task_id='prepare_task',
                       python_callable=_task_prepare,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task2 = PythonOperator(task_id='schema_converse_task',
                       python_callable=_task_converse_schema,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task3 = PythonOperator(task_id='data_converse_task',
                       python_callable=_task_converse_data,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task4 = PythonOperator(task_id='upload_result_task',
                       python_callable=_task_upload_result,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)

task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)
