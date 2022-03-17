import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import prepare as mysql2mongo_prepare
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import convert_schema as mysql2mongo_convert_schema
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import convert_data as mysql2mongo_convert_data
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import upload_converted_data as mysql2mongo_upload_data

logger = logging.getLogger(__name__)


def _task_prepare(**kwargs):
    file_info = {
        'sql_file_url': kwargs['dag_run'].conf.get('sql_file_url'),
        'resource_id': kwargs['dag_run'].conf.get('resource_id'),
        'sql_file_name': kwargs['dag_run'].conf.get('sql_file_name'),
    }
    mysql2mongo_prepare(**file_info)
    kwargs['ti'].xcom_push(key='input_file_info', value=file_info)


def _task_convert_schema(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_convert_schema(input_file_info['resource_id'], input_file_info['sql_file_name'])


def _task_convert_data(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_convert_data(input_file_info['resource_id'], input_file_info['sql_file_name'])


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


dag = DAG('data_conversion_flow',
          description='Data Conversion Flow',
          schedule_interval='0 12 * * *',
          start_date=datetime(2022, 3, 4), catchup=False)

task1 = PythonOperator(task_id='prepare_task',
                       python_callable=_task_prepare,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task2 = PythonOperator(task_id='schema_convert_task',
                       python_callable=_task_convert_schema,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task3 = PythonOperator(task_id='data_convert_task',
                       python_callable=_task_convert_data,
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
