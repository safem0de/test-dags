import time
import itertools
import requests
import logging
import json
from datetime import datetime

class ApiServices:
    def __init__(self, api_keys, rate_limit=5, reset_time=60):
        """
        ✅ ตัวจัดการ API Key ที่สามารถสลับการใช้งานได้โดยอัตโนมัติ
        :param api_keys: รายการ API Keys ที่ใช้งาน
        :param rate_limit: จำนวนครั้งสูงสุดที่เรียก API ได้ต่อ Key
        :param reset_time: เวลารอให้ API Key รีเซ็ต (วินาที)
        """
        self.api_keys = itertools.cycle(api_keys)  # 🔹 หมุน API Keys วนไปเรื่อยๆ
        self.rate_limit = rate_limit
        self.reset_time = reset_time

        # 🔹 โครงสร้างเก็บข้อมูล API Key และ Timestamp
        self.api_call_tracker = [{
            "key": key,
            "ts": {}                # 🔹 เก็บ timestamp ที่ใช้ API
        } for key in api_keys]

    def get_available_key(self):
        """✅ เลือก API Key ที่ยังไม่ติด Rate Limit"""
        current_time = time.time()

        for entry in self.api_call_tracker:
            key = entry["key"]

            # ✅ ลบ timestamp ที่เกิน RESET_TIME
            entry["ts"] = {k: v for k, v in entry["ts"].items() 
                           if current_time - datetime.strptime(v, "%Y-%m-%d %H:%M:%S").timestamp() < self.reset_time}

            # ✅ ถ้า Key ยังมีโควต้าเหลือ
            if len(entry["ts"]) < self.rate_limit:
                logging.info(f"✅ API Key {key} ใช้งานได้ ({len(entry['ts'])}/{self.rate_limit} calls used)")
                return entry  # 🔹 คืนค่า Entry ที่สามารถใช้ API ได้

        # ✅ ถ้าทุก API Key หมดโควต้า ให้รอแล้วลองใหม่
        logging.warning(f"⏳ ทุก API Key หมดโควต้า! รอ {self.reset_time} วินาที...")
        time.sleep(self.reset_time)
        return self.get_available_key()

    def fetch_api(self, url, params=None, max_retries=3):
        """✅ ดึงข้อมูล API และจัดการ Rate Limit"""
        retries = 0

        while retries < max_retries:
            entry = self.get_available_key()
            current_key = entry["key"]
            params = params or {}
            params["key"] = current_key

            try:
                response = requests.get(url, params=params)
                logging.info(f"🌐 API Request: {url} | Status: {response.status_code}")

                if response.status_code == 429:
                    logging.warning(f"⏳ API Key {current_key} ติด Rate Limit! รอ {self.reset_time} วินาที...")
                    time.sleep(self.reset_time)  # ✅ รอให้ API Key รีเซ็ต
                    retries += 1
                    continue  # ✅ ลองใหม่หลังจากรอครบเวลา

                response.raise_for_status()

                # ✅ บันทึก Timestamp การเรียกใช้ API
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ts_index = len(entry["ts"]) + 1
                entry["ts"][ts_index] = now_str

                return response.json()

            except requests.exceptions.RequestException as e:
                logging.error(f"❌ API Request ล้มเหลว: {e}")
                return None

        return None

    def get_api_usage(self):
        """✅ คืนค่าการใช้งาน API Key ในรูปแบบ JSON"""
        return json.dumps(self.api_call_tracker, indent=4)


# # ✅ ตั้งค่า API Keys และสร้าง Instance ของ APIManager
# API_KEYS = ["API_KEY_1", "API_KEY_2"]  # 🔹 ใส่ API Keys ที่มี
# api_manager = APIManager(API_KEYS, rate_limit=5, reset_time=60)

# # ✅ ตัวอย่างการเรียก API
# API_URL = "http://api.airvisual.com/v2/cities"

# for _ in range(10):  # 🔹 ลองเรียก API 10 ครั้ง
#     result = api_manager.fetch_api(API_URL, params={"state": "Bangkok", "country": "Thailand"})
#     print(result)

# # ✅ แสดงโครงสร้าง API Key ที่ใช้ไปแล้ว
# print(api_manager.get_api_usage())
