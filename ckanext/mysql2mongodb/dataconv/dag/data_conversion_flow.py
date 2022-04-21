import logging
from datetime import datetime

from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator

from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import prepare as mysql2mongo_prepare
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import convert_schema as mysql2mongo_convert_schema
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import convert_data as mysql2mongo_convert_data
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import dump_data as mysql2mongo_dump_data
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import upload_converted_data as mysql2mongo_upload_data
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import validate_data as mysql2mongo_validate_data
from ckanext.mysql2mongodb.dataconv.task.mysql_mongo import export_validator_report \
    as mysql2mongo_export_report

logger = logging.getLogger(__name__)


def _task_prepare(**kwargs):
    file_info = {
        k: kwargs['dag_run'].conf.get(k)
        for k in ('sql_file_url', 'resource_id', 'sql_file_name', 'package_id')
    }
    mysql2mongo_prepare(file_info['sql_file_url'],
                        file_info['resource_id'],
                        file_info['sql_file_name'])
    kwargs['ti'].xcom_push(key='input_file_info', value=file_info)


def _task_convert_schema(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_convert_schema(input_file_info['resource_id'], input_file_info['sql_file_name'])


def _task_convert_data(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_convert_data(input_file_info['sql_file_name'])


def _task_validate_data(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_validate_data(sql_file_name=input_file_info['sql_file_name'],
                              resource_id=input_file_info['resource_id'],
                              package_id=input_file_info['package_id'])


def _task_export_validator_report(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_export_report(resource_id=input_file_info['resource_id'],
                              package_id=input_file_info['package_id'])


def _task_dump_data(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_dump_data(input_file_info['resource_id'], input_file_info['sql_file_name'])


def _task_upload_result(**kwargs):
    input_file_info = kwargs['ti'].xcom_pull(task_ids='prepare_task', key='input_file_info')
    mysql2mongo_upload_data(input_file_info['resource_id'],
                            input_file_info['sql_file_name'],
                            input_file_info['package_id'])


def _task_end_of_the_road(**kwargs):
    raise AirflowException('End Of The Road')


dag = DAG('data_conversion_flow',
          description='Data Conversion Flow',
          schedule_interval=None,
          start_date=datetime(2022, 3, 4), catchup=False, max_active_runs=1)

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
task4 = PythonOperator(task_id='result_validation_task',
                       python_callable=_task_validate_data,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task5 = PythonOperator(task_id='data_dumping_task',
                       python_callable=_task_dump_data,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task6 = PythonOperator(task_id='validation_result_export_task',
                       python_callable=_task_export_validator_report,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task7 = PythonOperator(task_id='upload_result_task',
                       python_callable=_task_upload_result,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)
task8 = PythonOperator(task_id='stop_dag',
                       python_callable=_task_end_of_the_road,
                       op_kwargs={},
                       provide_context=True,
                       dag=dag)

task2.set_upstream(task1)
task3.set_upstream(task2)
task4.set_upstream(task3)
task5.set_upstream(task3)
task6.set_upstream(task4)
task7.set_upstream([task5, task6])
task8.set_upstream(task7)
