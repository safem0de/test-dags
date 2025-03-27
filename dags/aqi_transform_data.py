import logging
import os
import pandas as pd
import numpy as np
from dags.aqi_class.AirQualityDatabase import AirQualityDatabase
from dags.aqi_class.ApiServices import ApiServices
from dags.aqi_class.CommonServices import CommonServices
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

conn_id = "0_postgres_db"
pg_hook = PostgresHook(postgres_conn_id=conn_id)
conn = pg_hook.get_connection(conn_id)
dblink_conn_str = f"host={conn.host} port={conn.port} dbname={conn.schema} user={conn.login} password={conn.password}"
cms = CommonServices()

def _create_dim_location():
    print("🔰 Ingest dim location")
    sql = f"""
    INSERT INTO dim_location (city, state, country, region)
    SELECT * FROM dblink(
        '{dblink_conn_str}'::text,
        'SELECT DISTINCT city, state, country, region FROM air_quality_raw ORDER BY region, state, city'::text
    ) AS t(city TEXT, state TEXT, country TEXT, region TEXT)
    ON CONFLICT (city, state, country, region) DO NOTHING;
    """
    print(sql)
    cms.execute_sql(conn_id,"aqi_datawarehouse",sql)

def _create_dim_time():
    print("🔰 Ingest dim time")
    sql = f"""
    INSERT INTO dim_time (
        time_id, date, hour, day_of_week, month_name, quarter,week_of_year, is_weekend, is_holiday
    )
    SELECT DISTINCT
        TO_CHAR(timestamp, 'YYYYMMDDHH24')::BIGINT AS time_id,
        DATE(timestamp) AS date,
        DATE_PART('hour', timestamp) AS hour,
        TO_CHAR(timestamp, 'Day') AS day_of_week,
        TO_CHAR(timestamp, 'Month') AS month_name,
        DATE_PART('quarter', timestamp) AS quarter,
        DATE_PART('week', timestamp) AS week_of_year,
        CASE WHEN DATE_PART('dow', timestamp) IN (0, 6) THEN true ELSE false END AS is_weekend,
        false AS is_holiday
    FROM dblink(
        '{dblink_conn_str}'::text,
        'SELECT timestamp FROM air_quality_raw'
    ) AS t(timestamp TIMESTAMP)
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