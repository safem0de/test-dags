import csv
import json
import os

from aqi_class.CommonServices import CommonServices
from airflow.providers.postgres.hooks.postgres import PostgresHook

class AirQualityDatawarehouse:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self, conn_id: str, api_url: str, api_key: str, dag_file_path: str):
        self.conn_id = conn_id
        self.api_url = api_url
        self.api_key = api_key
        self.dag_file_path = dag_file_path
        self.cms = CommonServices()

    def create_aqi_datawarehouse(self):
        self.database_name = "aqi_datawarehouse"
        self.cms.create_database(self.conn_id, self.database_name)
    
    def create_aqi_dim_location(self):
        print("🔰 Start create dim location")
        sql = """
            CREATE TABLE IF NOT EXISTS dim_location (
                location_id SERIAL PRIMARY KEY,
                city VARCHAR(255) NOT NULL,
                state VARCHAR(255) NOT NULL,
                country VARCHAR(50) DEFAULT 'Thailand',
                region VARCHAR(255) NOT NULL,
                UNIQUE (city, state, country, region)
            );
        """
        self.cms.execute_sql(
            conn_id=self.conn_id, 
            database_name="aqi_datawarehouse", 
            sql_statement=sql
            )


    def create_aqi_dim_time(self):
        print("🔰 Start create dim time")
        sql = """
            CREATE TABLE IF NOT EXISTS dim_time (
                time_id SERIAL PRIMARY KEY,        -- Unique ID (Auto Increment)
                date DATE NOT NULL,                -- วันที่ (YYYY-MM-DD)
                hour INT NOT NULL CHECK (hour BETWEEN 0 AND 23),  -- ชั่วโมง (0-23)
                day_of_week VARCHAR(10) NOT NULL,  -- ชื่อวันในสัปดาห์ (Monday, Tuesday, etc.)
                month_name VARCHAR(10) NOT NULL,   -- ชื่อเดือน (January, February, etc.)
                quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4), -- ไตรมาส (1-4)
                week_of_year INT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53), -- สัปดาห์ของปี
                is_weekend BOOLEAN NOT NULL,       -- เป็นวันหยุดสุดสัปดาห์หรือไม่ (TRUE/FALSE)
                is_holiday BOOLEAN DEFAULT FALSE   -- เป็นวันหยุดพิเศษหรือไม่ (TRUE/FALSE)
            );
        """
        self.cms.execute_sql(
            conn_id=self.conn_id, 
            database_name="aqi_datawarehouse", 
            sql_statement=sql
            )
    

    def create_aqi_fact_table(self):
        print("🔰 Start create fact table")
        sql = """
            CREATE TABLE fact_air_quality (
            fact_id SERIAL PRIMARY KEY,         -- Auto-increment
            time_id INT NOT NULL,               -- FK to dim_time
            location_id INT NOT NULL,           -- FK to dim_location
            
            -- Pollution Data
            aqius INT NOT NULL,                 -- AQI (US Standard)
            mainus VARCHAR(10) NOT NULL,        -- Main pollutant (US)
            aqicn INT NOT NULL,                 -- AQI (China Standard)
            maincn VARCHAR(10) NOT NULL,        -- Main pollutant (China)
            
            -- Weather Data
            temperature INT NOT NULL,            -- Temperature (°C)
            pressure INT NOT NULL,               -- Atmospheric Pressure (hPa)
            humidity INT NOT NULL,               -- Humidity (%)
            wind_speed NUMERIC(5, 2) NOT NULL,   -- Wind Speed (m/s)
            wind_direction INT NOT NULL,         -- Wind Direction (°)
            weather_icon VARCHAR(10) NOT NULL,   -- Weather Condition Icon

            -- Foreign Keys
            FOREIGN KEY (time_id) REFERENCES dim_time (time_id),
            FOREIGN KEY (location_id) REFERENCES dim_location (location_id)
        );
        """
        self.cms.execute_sql(
            conn_id=self.conn_id, 
            database_name="aqi_datawarehouse", 
            sql_statement=sql
            )