import csv
import json
import os
import requests
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook


class AirQualityDatabase:
    """Class สำหรับจัดการ Database, API และการสร้างไฟล์ JSON"""

    def __init__(self, conn_id: str, api_url: str, api_key: str, dag_file_path: str):
        self.conn_id = conn_id
        self.api_url = api_url
        self.api_key = api_key
        self.dag_file_path = dag_file_path
        self.last_request_time = 0

        print(f"API Url: {self.api_url}")
        print(f"API Key: {self.api_key[:3]}******{self.api_key[-3:]}")

    def map_region(state):
        """
        คืนค่าภาค (region) ตามชื่อจังหวัด
        เหนือ	    North
        อีสาน	    Northeast
        กลาง	   Central
        ตะวันออก	East
        ตะวันตก     West
        ใต้         South
        """
        region_mapping = {
            "North": {'Chiang Mai', 'Chiang Rai', 'Lampang', 'Lamphun', 'Nan', 'Phayao', 'Phrae', 'Mae Hong Son', 'Tak', 'Uttaradit'},
            "Northeast": {'Amnat Charoen', 'Buriram', 'Chaiyaphum', 'Kalasin', 'Khon Kaen', 'Loei', 'Maha Sarakham', 'Mukdahan',
                    'Nakhon Phanom', 'Nakhon Ratchasima', 'Nong Bua Lamphu', 'Nong Khai', 'Roi Et', 'Sakon Nakhon', 
                    'Sisaket', 'Surin', 'Ubon Ratchathani', 'Udon Thani', 'Yasothon'},
            "Central": {'Ang Thong', 'Bangkok', 'Chai Nat', 'Kamphaeng Phet', 'Lopburi', 'Nakhon Nayok', 'Nakhon Pathom',
                    'Nakhon Sawan', 'Nonthaburi', 'Pathum Thani', 'Phetchabun', 'Phichit', 'Phitsanulok', 'Phra Nakhon Si Ayutthaya',
                    'Samut Prakan', 'Samut Sakhon', 'Samut Songkhram', 'Sara Buri', 'Sing Buri', 'Suphan Buri'},
            "East": {'Chachoengsao', 'Chanthaburi', 'Chon Buri', 'Prachin Buri', 'Rayong', 'Sa Kaeo', 'Trat'},
            "West": {'Kanchanaburi', 'Phetchaburi', 'Prachuap Khiri Khan', 'Ratchaburi', 'Tak'},
            "South": {'Chumphon', 'Krabi', 'Nakhon Si Thammarat', 'Narathiwat', 'Pattani', 'Phangnga', 'Phatthalung',
                    'Phuket', 'Ranong', 'Satun', 'Songkhla', 'Surat Thani', 'Trang', 'Yala'}
        }
        
        for region, states in region_mapping.items():
            if state in states:
                return region
        
        return "Unknown"  # Default ถ้าไม่พบข้อมูล


    def json_to_list(self, filename: str, parent_key: str, child_key: str) -> list:
        """
        Extracts a list of values from a JSON file based on the specified keys.

        :param filename: Path to the JSON file.
        :param parent_key: The key that contains the list of dictionaries.
        :param child_key: The key to extract values from each dictionary inside the parent_key list.
        :return: A list of extracted values.
        """
        try:
            # อ่าน JSON จากไฟล์
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)

            # ตรวจสอบว่า JSON เป็น dictionary และมี parent_key ที่ต้องการ
            if not isinstance(data, dict) or parent_key not in data:
                print(f"❌ Error: JSON file does not contain expected '{parent_key}' key.")
                return []

            # ตรวจสอบว่า parent_key มีข้อมูลเป็น list หรือไม่
            if not isinstance(data[parent_key], list):
                print(f"❌ Error: '{parent_key}' is not a list.")
                return []

            # ดึงค่าจาก child_key ในแต่ละ dictionary
            return [item.get(child_key, None) for item in data[parent_key]]

        except FileNotFoundError:
            print(f"❌ Error: File '{filename}' not found.")
            return []

        except json.JSONDecodeError:
            print(f"❌ Error: File '{filename}' is not a valid JSON file.")
            return []

    # ✅ ตรวจสอบ Connection ของ Database
    def check_conn_string(self):
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute("SELECT 1;")
            connection.close()
            print(f"✅ Connection {self.conn_id} is valid!")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {str(e)}")
            return False


    # ✅ Execute SQL Query
    def execute_sql(self, database_name: str, sql_statement: str):
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conn_id, database=database_name)
            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            cursor.execute(sql_statement)
            connection.commit()
            cursor.close()
            connection.close()
            print(f"✅ SQL executed successfully on database: {database_name}")
        except Exception as e:
            print(f"❌ Error executing SQL on {database_name}: {str(e)}")


    # ✅ สร้างไฟล์ JSON ถ้ายังไม่มี
    def create_file_if_not_exist(self, filename: str, data: dict):
        file_path = os.path.join(self.dag_file_path, filename)
        
        if not isinstance(data, dict):  # ตรวจสอบว่า data เป็น dict
            print("❌ Error: Provided data is not a valid JSON dictionary.")
            return

        if not os.path.exists(file_path):
            with open(file_path, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"✅ File created: {file_path}")
        else:
            print(f"⚠️ File already exists: {file_path}")


    def fetch_api(self, endpoint: str, rate_limit : int = 4, params: dict = None):
        """Fetch AQI data from API with dynamic parameters"""
        if not isinstance(rate_limit, int):
            raise ValueError(f"❌ rate_limit ต้องเป็น int แต่ได้รับ {type(rate_limit)}")

        request_interval = 60 / rate_limit  # เช่น 5 calls/min → รอ 12 วินาที/call, 4 calls/min → รอ 15 วินาที/call
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        # ✅ ถ้ายังไม่ถึงเวลาที่กำหนด ให้รอ
        if time_since_last_request < request_interval:
            wait_time = request_interval - time_since_last_request
            print(f"⏳ Waiting {wait_time:.2f} seconds before next API call...")
            time.sleep(wait_time)

        url = f"{self.api_url}{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            self.last_request_time = time.time() # บันทึกเวลาล่าสุดที่เรียก API
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ API Request failed: {e}")
            return None


    # ✅ สร้างฐานข้อมูล AQI ถ้ายังไม่มี
    def create_aqi_database(self):
        self.check_conn_string()
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)
        connection = pg_hook.get_conn()
        connection.set_isolation_level(0)
        cursor = connection.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'aqi_database';")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute("CREATE DATABASE aqi_database;")
            connection.commit()
        
        connection.close()
        print("✅ Database 'aqi_database' is ready!")


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
        self.execute_sql("aqi_database", sql)


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
        self.execute_sql("aqi_database", sql)


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
        self.execute_sql("aqi_database", sql)


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
        data = self.fetch_api(endpoint="v2/states", params=params)
        print(data)

        self.create_file_if_not_exist(filename, data)


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
        data = self.fetch_api(endpoint="v2/cities", params=params)
        print(data)

        self.create_file_if_not_exist(filename, data)


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
            region_name = self.map_region(state_name)  # ✅ หา region ตามจังหวัด
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
