# Instructions
# Define a function that uses the python logger to log a function. Then finish filling in the details of the DAG down below. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file or the video walkthrough on the next page.

import logging
import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from operators.load_parquet_to_redshift import LoadParquetToRedshift
from operators.data_quality import DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator

emrsshHook= SSHHook(ssh_conn_id='emr_ssh_connection')

default_args = {
    'owner': 'GopiShan',
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False, 
    'catchup':False
}

dag = DAG('capstone_project',
        default_args=default_args,
        description='Capstone project to load 4 different datasets using Spark, Redshift and Airflow',
        start_date=datetime.datetime.now())

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

copy_files_task = SSHOperator(
    task_id="copy_files_to_emr",
    command='export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;mkdir spark-job;cd /home/hadoop/spark-job;aws s3 cp s3://data-engineering-nd/capstone-project/scripts/dl.cfg /home/hadoop/spark-job/dl.cfg;aws s3 cp s3://data-engineering-nd/capstone-project/scripts/immigration_etl.py /home/hadoop/spark-job/immigration_etl.py;aws s3 cp s3://data-engineering-nd/capstone-project/scripts/airport_etl.py /home/hadoop/spark-job/airport_etl.py;aws s3 cp s3://data-engineering-nd/capstone-project/scripts/temperature_etl.py /home/hadoop/spark-job/temperature_etl.py;aws s3 cp s3://data-engineering-nd/capstone-project/scripts/demographics_etl.py /home/hadoop/spark-job/demographics_etl.py;',
    ssh_hook=emrsshHook,
    dag=dag)

immigration_etl_task = SSHOperator(
    task_id="immigration_etl",
    command='cd /home/hadoop/spark-job;spark-submit --master yarn --deploy-mode client immigration_etl.py;',
    ssh_hook=emrsshHook,
    dag=dag)

airport_etl_task = SSHOperator(
    task_id="airport_etl",
    command='cd /home/hadoop/spark-job;spark-submit --master yarn --deploy-mode client airport_etl.py;',
    ssh_hook=emrsshHook,
    dag=dag)

temperature_etl_task = SSHOperator(
    task_id="temperature_etl",
    command='cd /home/hadoop/spark-job;spark-submit --master yarn --deploy-mode client temperature_etl.py;',
    ssh_hook=emrsshHook,
    dag=dag)

demographics_etl_task = SSHOperator(
    task_id="demographics_etl",
    command='cd /home/hadoop/spark-job;spark-submit --master yarn --deploy-mode client demographics_etl.py;',
    ssh_hook=emrsshHook,
    dag=dag)

load_immig_fact_to_redshift = LoadParquetToRedshift(
    task_id='load_fact_immig',
    redshift_conn_id='redshift',
    table='immig_fact',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/immig_fact.parquet/',
    dag=dag
)

load_visa_type_dim_to_redshift = LoadParquetToRedshift(
    task_id='load_visa_type_dim',
    redshift_conn_id='redshift',
    table='visa_type_dim',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/visa_type_dim.parquet/',
    dag=dag
)

load_travel_mode_dim_to_redshift = LoadParquetToRedshift(
    task_id='load_travel_mode_dim',
    redshift_conn_id='redshift',
    table='travel_mode_dim',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/travel_mode_dim.parquet/',
    dag=dag
)

load_time_dim_to_redshift = LoadParquetToRedshift(
    task_id='load_time_dim',
    redshift_conn_id='redshift',
    table='time_dim',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/time_dim.parquet/',
    dag=dag
)

load_airport_dim_to_redshift = LoadParquetToRedshift(
    task_id='load_airport_dim',
    redshift_conn_id='redshift',
    table='airport_dim',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/airport_dim.parquet/',
    dag=dag
)

load_country_data_to_redshift = LoadParquetToRedshift(
    task_id='load_country_temperature_dim',
    redshift_conn_id='redshift',
    table='country_temperature_dim',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/country_dim.parquet/',
    dag=dag
)

load_demographics_dim_to_redshift = LoadParquetToRedshift(
    task_id='load_demographics_dim',
    redshift_conn_id='redshift',
    table='demographics_dim',
    aws_credentials_id='aws_credentials',
    s3_bucket='data-engineering-nd',
    s3_key='capstone-project/output/demographics_dim.parquet/',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='run_quality_checks',
    redshift_connection_id = 'redshift',
    target_table=("immig_fact","visa_type_dim","travel_mode_dim","time_dim","airport_dim","country_temperature_dim","demographics_dim"),
    validate_column=("cicid","visa_category_id","travel_mode_id","arrival_sas","airport_code","country_code","stateCode"),
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> copy_files_task
copy_files_task >> [immigration_etl_task, airport_etl_task, temperature_etl_task, demographics_etl_task]
[immigration_etl_task, airport_etl_task, temperature_etl_task, demographics_etl_task] >> load_immig_fact_to_redshift
load_immig_fact_to_redshift >> load_airport_dim_to_redshift
load_airport_dim_to_redshift >> [load_visa_type_dim_to_redshift, load_travel_mode_dim_to_redshift, load_time_dim_to_redshift, load_country_data_to_redshift, load_demographics_dim_to_redshift]
[load_visa_type_dim_to_redshift, load_travel_mode_dim_to_redshift, load_time_dim_to_redshift, load_country_data_to_redshift, load_demographics_dim_to_redshift] >> run_quality_checks
run_quality_checks >> end_operator