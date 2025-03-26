import logging
import os
import pandas as pd
import numpy as np
from dags.aqi_class.AirQualityDatabase import AirQualityDatabase
from dags.aqi_class.ApiServices import ApiServices
from dags.aqi_class.CommonServices import CommonServices
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

conn_id = "0_postgres_db"
cms = CommonServices()

def _create_dim_location():
    print("🔰 Ingest dim location")
    sql = """
    INSERT INTO dim_location (city, state, country, region)
    SELECT DISTINCT city, state, country, region
    FROM public.air_quality_raw
    ON CONFLICT (city, state, country, region) DO NOTHING;
    """
    cms.execute_sql(conn_id,"aqi_datawarehouse",sql)

with DAG(
    "airquality_transform_data",
    schedule=None,
    start_date=timezone.datetime(2025, 3, 20),
    tags=["capstone","datawarehouse"]
):
    start = EmptyOperator(task_id="start")

    create_dim_location = PythonOperator(
        task_id="create_dim_location",
        python_callable=_create_dim_location,
    )

    end = EmptyOperator(task_id="end")

    start >> create_dim_location >> end