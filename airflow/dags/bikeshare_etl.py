from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
import datetime, logging


def check_month_data_availability(*args, **kwargs):
    execution_date = datetime.datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    bucket_name = Variable.get('bikeshare_bucket_name')
    s3_address = Variable.get('bikeshare_s3_address')
    current_month_object_key = s3_address + f'year={execution_date.year}/month={execution_date.month}/divvy_trips.csv'
    # current_month_object_key = s3_address + f'year=2018/month=2/divvy_trips.csv'
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    file_exists = s3_hook.check_for_key(key=current_month_object_key,
                                        bucket_name=bucket_name)
    if file_exists:
        logging.info(f'File {execution_date.year}/{execution_date.month} exists.')
    else:
        raise Exception



def copy_data_to_redshift():
    pass


def update_athena_partition():
    pass


def check_data_in_redshift():
    pass


etl_dag = DAG(
    'Bikeshare_ETL',
    start_date=datetime.datetime.now() - datetime.timedelta(days=600),
    schedule_interval='@daily'
)


source_data_check = PythonOperator(
    task_id='Current_month_existence_check.task',
    python_callable=check_month_data_availability,
    provide_context=True,
    dag=etl_dag
)

