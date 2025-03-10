import csv
import json
import os

from dags.aqi_class.CommonServices import CommonServices
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
            CREATE TABLE IF NOT EXISTS location (
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