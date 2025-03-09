import time
import itertools
import requests
import logging
import json
from datetime import datetime

class ApiServices:
    def __init__(self, api_keys:list, rate_limit:int=5, reset_time:int=60):
        """
        ✅ ตัวจัดการ API Key ที่สามารถสลับการใช้งานได้โดยอัตโนมัติ
        :param api_keys: รายการ API Keys ที่ใช้งาน
        :param rate_limit: จำนวนครั้งสูงสุดที่เรียก API ได้ต่อ Key
        :param reset_time: เวลารอให้ API Key รีเซ็ต (วินาที)
        """
        self.keys = api_keys
        self.api_keys = itertools.cycle(api_keys)  # 🔹 หมุน API Keys วนไปเรื่อยๆ
        self.rate_limit = rate_limit
        self.reset_time = reset_time
        self.api_usage = {key: [] for key in api_keys}

        self.current_key = next(api_keys)

    def get_available_key(self):
        current_time = time.time()

        for _ in range(len(self.keys)):  # ✅ ลองสลับ API Key ทั้งหมด
            timestamps = self.api_usage[current_key]
            # ลบ timestamp ที่เกิน RESET_TIME ออก
            self.api_usage[current_key] = [ts for ts in timestamps if current_time - ts < self.reset_time]

            if len(self.api_usage[current_key]) < 5:
                return current_key
            else:
                print(f"⏳ API Key {current_key} ติด Rate Limit! ลองสลับไปใช้ API Key อื่น...")
                current_key = next(self.keys)  # ✅ เปลี่ยนไปใช้ API Key ตัวถัดไป

        print("⚠️ ไม่มี API Key ที่ใช้งานได้! ต้องรอ 60 วินาที...")
        time.sleep(60)  # ❗ รอ 60 วินาที แล้วลองใหม่
        return self.get_available_key()

    def fetch_api(self, url, params=None, max_retries=3):
        """✅ ดึงข้อมูล API และจัดการ Rate Limit"""
        retries = 0
        wait_time = self.reset_time/max_retries

        while retries < max_retries:
            entry = self.get_available_key()
            current_key = entry["key"]
            params = params or {}
            params["key"] = current_key

            try:
                response = requests.get(url, params=params)
                logging.info(f"🌐 API Request: {url} | Status: {response.status_code}")

                if response.status_code == 429:
                    logging.warning(f"⏳ API Key {current_key} ติด Rate Limit! รอ {wait_time} วินาที...")
                    time.sleep(wait_time)  # ✅ รอให้ API Key รีเซ็ต
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