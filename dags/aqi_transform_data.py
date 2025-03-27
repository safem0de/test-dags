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
    SELECT * FROM dblink(
        'host=43.209.49.162 port=30432 dbname=aqi_database user=airflow password=airflow'::text,
        'SELECT DISTINCT city, state, country, region FROM air_quality_raw ORDER BY region, state, city'::text
    ) AS t(city TEXT, state TEXT, country TEXT, region TEXT)
    ON CONFLICT (city, state, country, region) DO NOTHING;
    """
    cms.execute_sql(conn_id,"aqi_datawarehouse",sql)

def _create_dim_time():
    print("🔰 Ingest dim location")
    sql = """
    INSERT  INTO dim_time (time_id, date, hour, day_of_week, month_name, quarter, week_of_year, is_weekend, is_holiday)
    SELECT	DISTINCT 
            DATE(timestamp) AS date
            ,DATE_PART('hour', timestamp) AS hour
            ,TO_CHAR(timestamp,	'Day') AS day_of_week
            ,TO_CHAR(timestamp,	'Month') AS month_name
            ,DATE_PART('quarter', timestamp) AS quarter
            ,DATE_PART('week', timestamp) AS week_of_year
            ,CASE WHEN DATE_PART('dow', timestamp) IN (0, 6) THEN true ELSE false END AS is_weekend
            ,false AS is_holiday
    FROM 	air_quality_raw
    ON CONFLICT (time_id) DO NOTHING;
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

    create_dim_time = PythonOperator(
        task_id="create_dim_time",
        python_callable=_create_dim_time,
    )

    end = EmptyOperator(task_id="end")

    start >> create_dim_location >> create_dim_time >> end