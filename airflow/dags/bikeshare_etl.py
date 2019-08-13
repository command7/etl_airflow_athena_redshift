from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.contrib.hooks.aws_athena_hook import AWSAthenaHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
import datetime, logging
from uuid import uuid4
import time


class ETL_Exception(Exception):
    """Custom Exceptions for Bikeshare ETL Pipeline"""
    pass


class Month_Data_Missing(ETL_Exception):
    """Raised if trips data for the current month is missing."""
    pass


create_trips_table_sql = """
CREATE TABLE IF NOT EXISTS TRIPS (
    trip_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    bikeid INTEGER NOT NULL,
    tripduration DECIMAL(16, 2) NOT NULL,
    from_station_id INTEGER NOT NULL,
    from_station_name VARCHAR(100) NOT NULL,
    to_station_id INTEGER NOT NULL,
    to_station_name VARCHAR(100) NOT NULL,
    usertype VARCHAR(20) NOT NULL,
    gender VARCHAR(6) NOT NULL,
    birthyear SMALLINT NOT NULL,
    PRIMARY KEY(trip_id))
    DISTSTYLE ALL;
"""


copy_all_trips_sql = """
COPY {}
FROM '{}'
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
IGNOREHEADER 1
DELIMITER ','
"""


def check_month_data_availability(*args, **kwargs):
    execution_date = datetime.datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    bucket_name = Variable.get('bikeshare_bucket_name')
    s3_prefix = Variable.get('bikeshare_s3_prefix')
    current_month_object_key = s3_prefix + f'year={execution_date.year}/month={execution_date.month}/divvy_trips.csv'
    # current_month_object_key = s3_prefix + f'year=2018/month=2/divvy_trips.csv'
    s3_hook = S3Hook(aws_conn_id='aws_credentials')
    file_exists = s3_hook.check_for_key(key=current_month_object_key,
                                        bucket_name=bucket_name)
    if file_exists:
        logging.info(f'File {execution_date.year}/{execution_date.month} exists.')
    else:
        logging.error(f"Data for the year - {str(execution_date.year)}, month - {str(execution_date.month)} is missing")
        raise Month_Data_Missing


def copy_data_to_redshift(*args, **kwargs):
    execution_date = datetime.datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    aws_hook = AwsHook(aws_conn_id='aws_credentials')
    s3_prefix = Variable.get('bikeshare_s3_prefix')
    credentials = aws_hook.get_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    table_name = 'trips'
    s3_file_location = 's3://bikeshare-data-copy/' + s3_prefix + f'year={execution_date.year}/month={execution_date.month}/divvy_trips.csv'
    redshift_hook = PostgresHook('redshift_connection')
    redshift_hook.run(copy_all_trips_sql.format(table_name, s3_file_location, access_key, secret_key))

def update_athena_partition(*args, **kwargs):
    execution_date = datetime.datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    execution_month = execution_date.month
    execution_year = execution_date.year
    s3_prefix = Variable.get('bikeshare_s3_prefix')
    bucket_name = Variable.get('bikeshare_bucket_name')
    athena_table_name = Variable.get('bikeshare_athena_table')
    file_location = 's3://bikeshare-data-copy/' + s3_prefix + f'year={execution_year}/month={execution_month}/'
    result_configuration = {"OutputLocation": "s3://{}/".format(bucket_name)}
    partition_update_query = """
    ALTER TABLE {} add partition (year="{}", month='{}')
    location "{}";
    """
    athena_hook = AWSAthenaHook(aws_conn_id='aws_credentials')
    athena_hook.run_query(partition_update_query.format(athena_table_name,
                                                        execution_year,
                                                        execution_month,
                                                        file_location),
                          result_configuration=result_configuration,
                          query_context={"Database": "bikeshare_data"},
                          client_request_token=str(uuid4()))


def check_data_in_redshift(*args, **kwargs):
    execution_date = datetime.datetime.strptime(kwargs['ds'], '%Y-%m-%d')
    execution_month = execution_date.month
    execution_year = execution_date.year
    bucket_name = Variable.get("bikeshare_bucket_name")
    num_records_athena_query = f"""
    SELECT COUNT(*) FROM trips
    WHERE year = {execution_year} AND month = {execution_month}
    """
    num_records_redshift_query = f"""
    SELECT COUNT(*) FROM trips
    WHERE date_part(year, trips.start_time) = {execution_year} AND date_part(month, trips.start_time) = {execution_month}
    """
    # athena_hook = AWSAthenaHook(aws_conn_id='aws_credentials')
    # query_id = athena_hook.run_query(query=num_records_athena_query,
    #                                  query_context={"Database": "bikeshare_data"},
    #                                  result_configuration={"OutputLocation": "s3://{}/".format(bucket_name)},
    #                                  client_request_token=str(uuid4()))
    # time.sleep(20)
    # athena_query_results = athena_hook.get_query_results(query_execution_id=query_id)
    num_records_in_s3 = int(athena_query_results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'])
    redshift_hook = PostgresHook('redshift_connection')
    redshift_query_results = redshift_hook.get_records(sql=num_records_redshift_query)
    for key, value in redshift_query_results:
        logging.info(f"{key} - {value}")




etl_dag = DAG(
    'Bikeshare_ETL',
    start_date=datetime.datetime.strptime('2018-01-01', '%Y-%m-%d'),
    schedule_interval='@monthly'
)

#
# source_data_check = PythonOperator(
#     task_id='Current_month_existence_check.task',
#     python_callable=check_month_data_availability,
#     provide_context=True,
#     dag=etl_dag
# )
#
#
# trips_table_creation = PostgresOperator(
#     task_id='Create_trips_table.task',
#     postgres_conn_id='redshift_connection',
#     sql=create_trips_table_sql
# )
#
#
# copy_trips_data = PythonOperator(
#     task_id="Copy_data_to_redshift.task",
#     python_callable=copy_data_to_redshift,
#     provide_context=True,
#     dag=etl_dag
# )
#
#
# update_athena_meta_store = PythonOperator(
#     task_id="Update_Athena_Metastore.task",
#     python_callable=update_athena_partition,
#     dag=etl_dag,
#     provide_context=True
# )

validate_etl = PythonOperator(
    task_id="Validate_ETL_data.task",
    python_callable=check_data_in_redshift,
    provide_context=True,
    dag=etl_dag
)


# source_data_check >> trips_table_creation
# trips_table_creation >> copy_trips_data
# copy_trips_data >> update_athena_meta_store
