import logging

from airflow.decorators import dag
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)
minio_connection_info = BaseHook.get_connection('minio_de_challenge')
default_args = {
    'owner': 'renaros',
    'depends_on_past': False,
    'start_date': datetime(2024,1,1),
    'email': ['rossi.renato@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag_documentation_md = ""


def daily_or_history_decider(**context) -> str:
    """
    Function to decide if runs normal (daily) processing or full history load
    @param context: dag context sent by Airflow
    @return string containing task id to be executed
    """
    is_historical_processing = context['dag_run'].conf.get('is_historical_processing')
    if is_historical_processing is None or is_historical_processing == False:
        return 'save_api_daily_results'
    else:
        return 'save_api_historical_results'

## DAG ##
@dag(schedule="@daily", default_args=default_args, catchup=False, doc_md=dag_documentation_md, tags=["challenge", "fire-incidents"])
def dag_load_fire_incidents():

    # Checks if API is available
    api_sensor_task = HttpSensor(
        task_id='api_sensor',
        http_conn_id='',
        endpoint='https://data.sfgov.org/resource/wr8u-xric.json',
        method='GET',
        response_check=lambda response: True if response.status_code == 200 else False,
        poke_interval=60,
        timeout=120,
    )

    # Decide for daily or full history processing
    decide_daily_or_history_task = BranchPythonOperator(
        task_id='decide_daily_or_history',
        python_callable=daily_or_history_decider,
        provide_context=True
    )

    # Call pyspark script to process one day (based on execution day)
    save_api_daily_results_task = SparkSubmitOperator(
        task_id="save_api_daily_results",
        application="./dags/pyspark_scripts/get_fire_incidents_api_data.py",
        conn_id="spark_conn",
        application_args=["{{ds}}"],
        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.hadoop.fs.s3a.access.key": minio_connection_info.login,
            "spark.hadoop.fs.s3a.secret.key": minio_connection_info.password,
            "spark.hadoop.fs.s3a.endpoint": f"http://{minio_connection_info.host}:{minio_connection_info.port}",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4"
    )

    # Call pyspark script to process full history
    save_api_historical_results_task = SparkSubmitOperator(
        task_id="save_api_historical_results",
        application="./dags/pyspark_scripts/get_fire_incidents_api_data.py",
        conn_id="spark_conn",
        conf={
            "spark.driver.maxResultSize": "20g",
            "spark.hadoop.fs.s3a.access.key": minio_connection_info.login,
            "spark.hadoop.fs.s3a.secret.key": minio_connection_info.password,
            "spark.hadoop.fs.s3a.endpoint": f"http://{minio_connection_info.host}:{minio_connection_info.port}",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.path.style.access": "true"
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4"
    )

    # DAG workflow
    api_sensor_task >> decide_daily_or_history_task
    decide_daily_or_history_task >> save_api_daily_results_task
    decide_daily_or_history_task >> save_api_historical_results_task

dag_load_fire_incidents()