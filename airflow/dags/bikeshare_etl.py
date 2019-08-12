from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
import datetime, logging


def check_month_data_availability(*args, **kwargs):
    execution_date = kwargs['ds']
    date_time_format = datetime.datetime.strptime(execution_date, '%Y-%m-%d')
    logging.info(f'Type is {type(date_time_format)}')
    logging.info(f"Execution date : {date_time_format}")


def copy_data_to_redshift():
    pass


def update_athena_partition():
    pass


def check_data_in_redshift():
    pass


etl_dag = DAG(
    'Bikeshare_ETL',
    start_date=datetime.datetime.now(),
)


source_data_check = PythonOperator(
    task_id='Current_month_existence_check.task',
    python_callable=check_month_data_availability,
    provide_context=True,
    dag=etl_dag
)

