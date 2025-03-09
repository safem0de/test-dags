import csv
import json
import os

from dags.aqi_class.CommonServices import CommonServices
from airflow.providers.postgres.hooks.postgres import PostgresHook


class AirQualityDatabase:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self, conn_id: str, api_url: str, api_key: str, dag_file_path: str):
        self.conn_id = conn_id
        self.api_url = api_url
        self.api_key = api_key
        self.dag_file_path = dag_file_path
        self.cms = CommonServices()
        
        print(f"API Url: {self.api_url}")
        print(f"API Key: {self.api_key[:3]}******{self.api_key[-3:]}")


    # ✅ สร้างฐานข้อมูล AQI ถ้ายังไม่มี
    def create_aqi_database(self):
        # self.cms.check_conn_string()
        # pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        # connection = pg_hook.get_conn()
        # connection.set_isolation_level(0)
        # cursor = connection.cursor()

        # cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'aqi_database';")
        # exists = cursor.fetchone()
        
        # if not exists:
        #     cursor.execute("CREATE DATABASE aqi_database;")
        #     connection.commit()
        
        # connection.close()
        # print("✅ Database 'aqi_database' is ready!")
        self.database_name = "aqi_database"
        self.cms.create_database(self.conn_id, self.database_name)


    # ✅ สร้างตาราง location
    def create_aqi_table_location(self):
        sql = """
            CREATE TABLE IF NOT EXISTS location (
                location_id SERIAL PRIMARY KEY,
                city VARCHAR(255) NOT NULL,
                state VARCHAR(255) NOT NULL,
                country VARCHAR(50) DEFAULT 'Thailand',
                latitude DECIMAL(10, 6),
                longitude DECIMAL(10, 6),
                region VARCHAR(255) NOT NULL,
                UNIQUE (city, state, country)
            );
        """
        self.cms.execute_sql("aqi_database", sql)


    # ✅ สร้างตาราง aqi_data
    def create_aqi_table_aqi_data(self):
        sql = """
            CREATE TABLE IF NOT EXISTS aqi_data (
                aqi_id SERIAL PRIMARY KEY,
                location_id INT,
                timestamp TIMESTAMP NOT NULL,
                aqius INT NOT NULL, 
                mainus VARCHAR(10),
                aqicn INT,
                maincn VARCHAR(10),
                FOREIGN KEY (location_id) REFERENCES location(location_id) ON DELETE CASCADE
            );
        """
        self.cms.execute_sql("aqi_database", sql)


    # ✅ สร้างตาราง weather_data
    def create_aqi_table_weather_data(self):
        sql = """
            CREATE TABLE IF NOT EXISTS weather_data (
                weather_id SERIAL PRIMARY KEY,
                location_id INT,
                timestamp TIMESTAMP NOT NULL,
                temperature DECIMAL(5,2),
                pressure INT,
                humidity INT,
                wind_speed DECIMAL(5,2),
                wind_direction INT,
                FOREIGN KEY (location_id) REFERENCES location(location_id) ON DELETE CASCADE
            );
        """
        self.cms.execute_sql("aqi_database", sql)


    # ✅ ดึงข้อมูล state (จังหวัด) จาก API และเก็บเป็นไฟล์ JSON
    def get_state_data(self, filename="state_master.json"):
        file_path = os.path.join(self.dag_file_path, filename)

        # ถ้าไฟล์มีอยู่แล้ว ไม่ต้องดึงข้อมูล
        if os.path.exists(file_path):
            print(f"✅ File '{file_path}' already exists. Skipping API call.")
            return

        params = {
            "country": "thailand",
            "key": self.api_key
        }
        data = self.cms.fetch_api(endpoint="v2/states", params=params)
        print(data)

        self.cms.create_file_if_not_exist(filename, data)


    # ✅ ดึงข้อมูล city (อำเภอ) ตามจังหวัดจาก API
    def get_city_data(self, state_name):
        filename = f"city_master_{state_name}.json"
        file_path = os.path.join(self.dag_file_path, filename)

        # ถ้าไฟล์มีอยู่แล้ว ไม่ต้องดึงข้อมูล
        if os.path.exists(file_path):
            print(f"✅ File '{file_path}' already exists. Skipping API call.")
            return

        params = {
            "state": state_name,
            "country": "thailand",
            "key": self.api_key
        }
        data = self.cms.fetch_api(endpoint="v2/cities", params=params)
        print(data)

        self.cms.create_file_if_not_exist(filename, data)


    def generate_state_city_region_csv(self, dag_file_path, state_master, output_filename):
        """
        อ่านข้อมูลจาก `state_master.json` และ `city_master_xxx.json`
        แล้วบันทึกเป็นไฟล์ CSV ในรูปแบบ state, city
        """

        state_file = os.path.join(dag_file_path, state_master)
        output_file = os.path.join(dag_file_path, output_filename)

        # ✅ ตรวจสอบว่าไฟล์ state_master.json มีอยู่จริง
        if not os.path.exists(state_file):
            print(f"❌ Error: File '{state_file}' not found.")
            return

        # ✅ อ่านข้อมูล state_master.json
        with open(state_file, "r", encoding="utf-8") as f:
            state_data = json.load(f)

        # ✅ ตรวจสอบโครงสร้าง JSON
        if "data" not in state_data or not isinstance(state_data["data"], list):
            print(f"❌ Error: Invalid JSON structure in '{state_file}'.")
            return

        # ✅ เตรียมเก็บข้อมูล state, city
        state_city_list = []

        # ✅ วนลูปแต่ละจังหวัดเพื่ออ่านข้อมูลเมืองจาก city_master_xxx.json
        for state_entry in state_data["data"]:
            state_name = state_entry["state"]
            region_name = self.cms.map_region(state_name)  # ✅ หา region ตามจังหวัด
            city_file = os.path.join(dag_file_path, f"city_master_{state_name}.json")

            # ✅ ตรวจสอบว่าไฟล์ city_master_xxx.json มีอยู่จริง
            if os.path.exists(city_file):
                with open(city_file, "r", encoding="utf-8") as f:
                    city_data = json.load(f)

                # ✅ ตรวจสอบโครงสร้าง JSON
                if "data" in city_data and isinstance(city_data["data"], list):
                    cities = [city_entry["city"] for city_entry in city_data["data"]]
                else:
                    cities = []
            else:
                print(f"⚠️ Warning: File '{city_file}' not found. Skipping...")
                cities = []

            # ✅ เพิ่มข้อมูล state, city ลงใน list
            for city in cities:
                state_city_list.append([state_name, city, region_name])

        # ✅ เขียนข้อมูลลงไฟล์ CSV
        with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["state", "city", "region"])  # ✅ เขียน Header
            writer.writerows(state_city_list)   # ✅ เขียนข้อมูล

        print(f"✅ File saved: {output_file}")
