import logging

from airflow.decorators import dag, task
# from airflow.hooks.base import BaseHook
# from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta
# from minio import Minio
from sodapy import Socrata

logger = logging.getLogger(__name__)
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



## DAG ##
@dag(schedule="@daily", default_args=default_args, catchup=False, doc_md=dag_documentation_md, tags=["challenge", "fire-incidents"])
def dag_load_fire_incidents():

    # start_task = DummyOperator(task_id='start_task', dag=dag)
    # end_task = DummyOperator(task_id='end_task', dag=dag)

    api_sensor_task = HttpSensor(
        task_id='api_sensor',
        http_conn_id='fire_incidents_api',
        endpoint='',
        method='GET',
        response_check=lambda response: True if response.status_code == 200 else False,
        poke_interval=60,
        timeout=120,
    )

    @task
    def call_api_and_store_results(**context):
        # minio_connection_info = BaseHook.get_connection('minio_de_challenge')
        # minio_client = Minio(
        #     f"http://{minio_connection_info.host}:{minio_connection_info.port}", 
        #     access_key=minio_connection_info.login, 
        #     secret_key=minio_connection_info.password, 
        #     secure=False
        # )

        client = Socrata("data.sfgov.org", None)
        results = client.get("wr8u-xric", limit=2000)

        logger.info("Result of the API is:")
        logger.info(results)
    call_api_and_store_results_task = call_api_and_store_results()


    # DAG workflow
    api_sensor_task >> call_api_and_store_results_task
    # start_task >> api_sensor_task >> call_api_and_store_results_task >> end_task

dag_load_fire_incidents()