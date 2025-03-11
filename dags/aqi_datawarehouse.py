import os
from dags.aqi_class.AirQualityDatawarehouse import AirQualityDatawarehouse
from dags.aqi_class.CommonServices import CommonServices
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

# ตั้งค่าพารามิเตอร์
conn_id = "0_postgres_db"
api_url = Variable.get("air_quality_url")
api_key = Variable.get("air_quality_key_dwh")
dag_file_path = "/opt/airflow/dags/"

aqi_dwh = AirQualityDatawarehouse(conn_id, api_url, api_key, dag_file_path)
cms = CommonServices()

def _create_aqi_datawarehouse():
    aqi_dwh.create_aqi_datawarehouse()

def _create_aqi_dim_location():
    aqi_dwh.create_aqi_dim_location()

def _create_aqi_dim_time():
    aqi_dwh.create_aqi_dim_time()

def _create_aqi_fact_table():
    aqi_dwh.create_aqi_fact_table()

with DAG(
    "airquality_datawarehouse",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 8),
    tags=["capstone","datawarehouse"]
):
    start = EmptyOperator(task_id="start")

    create_aqi_datawarehouse = PythonOperator(
        task_id="create_aqi_datawarehouse",
        python_callable=_create_aqi_datawarehouse,
    )

    create_aqi_dim_location = PythonOperator(
        task_id="create_aqi_dim_location",
        python_callable=_create_aqi_dim_location,
    )

    create_aqi_dim_time = PythonOperator(
        task_id="create_aqi_dim_time",
        python_callable=_create_aqi_dim_time,
    )

    create_aqi_fact_table = PythonOperator(
        task_id="create_aqi_fact_table",
        python_callable=_create_aqi_fact_table,
    )

    end = EmptyOperator(task_id="end")

    start >> create_aqi_datawarehouse >> create_aqi_dim_time >> create_aqi_fact_table >> end