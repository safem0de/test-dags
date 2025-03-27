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

def _create_fact_table():
    print("🔰 Ingest fact table")
    sql = f"""
    INSERT INTO fact_air_quality (fact_id, time_id, location_id,
        aqius, mainus, aqicn, maincn, temperature, pressure, humidity,
        wind_speed, wind_direction, weather_icon
    )
    SELECT
        raw.aqi_id AS fact_id,
        TO_CHAR(raw.timestamp, 'YYYYMMDDHH24')::BIGINT AS time_id,
        loc.location_id AS location_id,
        raw.aqius, raw.mainus, raw.aqicn, raw.maincn,
        raw.temperature, raw.pressure, raw.humidity,
        raw.wind_speed, raw.wind_direction,
        '-' AS weather_icon
    FROM dblink(
        '{dblink_conn_str}'::text,
        'SELECT aqi_id, timestamp, city, state, country, region, aqius, mainus, aqicn, maincn, temperature, pressure, humidity, wind_speed, wind_direction FROM air_quality_raw'
    ) AS raw(
        aqi_id BIGINT,
        timestamp TIMESTAMP,
        city TEXT,
        state TEXT,
        country TEXT,
        region TEXT,
        aqius INTEGER,
        mainus TEXT,
        aqicn INTEGER,
        maincn TEXT,
        temperature FLOAT,
        pressure FLOAT,
        humidity FLOAT,
        wind_speed FLOAT,
        wind_direction FLOAT
    )
    JOIN dim_location loc
    ON raw.city = loc.city
    AND raw.state = loc.state
    AND raw.country = loc.country
    AND raw.region = loc.region
    ON CONFLICT (fact_id) DO NOTHING;
    """
    cms.execute_sql(conn_id,"aqi_datawarehouse",sql)

with DAG(
    "airquality_transform_data",
    schedule_interval="30 0 * * *",  # ✅ รันทุกวันเวลา 00:30
    start_date=timezone.datetime(2025, 3, 27),
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

    create_fact_table = PythonOperator(
        task_id="create_fact_table",
        python_callable=_create_fact_table,
    )

    end = EmptyOperator(task_id="end")

    start >> create_dim_location >> create_dim_time >> create_fact_table >> end