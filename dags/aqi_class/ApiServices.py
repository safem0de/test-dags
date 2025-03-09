import itertools
import time
import requests
from datetime import datetime

class ApiService:
    RESET_TIME = 60  # ⏳ เวลาที่ต้องรอเมื่อ API Key ติด Rate Limit
    RATE_LIMIT = 5  # 🔹 จำกัดจำนวนครั้งที่ API Key ใช้ได้ก่อนจะติด Rate Limit

    def __init__(self, api_keys):
        self.api_keys = itertools.cycle(api_keys)  # ✅ หมุน API Key ไปเรื่อยๆ
        self.api_usage = {key: [] for key in api_keys}  # ✅ บันทึก timestamp ของแต่ละ API Key
        self.current_key = next(self.api_keys)  # ✅ เริ่มต้นใช้ API Key ตัวแรก

    def _get_valid_api_key(self):
        """🔍 เลือก API Key ที่ยังมีโควต้าใช้งานได้"""
        current_time = time.time()

        for _ in range(len(self.api_usage)):  # ✅ วนลูปเช็ค API Key ทุกตัว
            timestamps = self.api_usage[self.current_key]
            # 🔹 ลบ timestamp ที่หมดอายุเกิน RESET_TIME ออก
            self.api_usage[self.current_key] = [ts for ts in timestamps if current_time - ts < self.RESET_TIME]

            if len(self.api_usage[self.current_key]) < self.RATE_LIMIT:
                return self.current_key  # ✅ ใช้ API Key นี้ได้
            else:
                print(f"⏳ API Key {self.current_key} ติด Rate Limit! ลองเปลี่ยนไปใช้ API Key อื่น...")
                self.current_key = next(self.api_keys)  # 🔁 เปลี่ยนไปใช้ API Key ถัดไป

        print("⚠️ ไม่มี API Key ที่ใช้งานได้! ต้องรอ 60 วินาที...")
        time.sleep(self.RESET_TIME)  # ❗ รอ 60 วินาที แล้วลองใหม่
        return self._get_valid_api_key()

    def fetch_api(self, url:str ,params:dict = None):
        """🌍 เรียก API พร้อมจัดการ Rate Limit & Error"""
        while True:
            self.current_key = self._get_valid_api_key()  # ✅ ใช้ API Key ที่ใช้ได้

            print(f"🌐 ใช้ API Key: {self.current_key} | Request: {url}")
            params["key"] = self.current_key
            response = requests.get(url, params)

            if response.status_code == 200:
                print(f"✅ สำเร็จ! Data: {response.json()}")
                self.api_usage[self.current_key].append(time.time())  # ✅ บันทึก timestamp
                return response.json()
            elif response.status_code == 400:
                print(f"❌ API Error 400: Bad Request! ลองเช็คพารามิเตอร์")
                return None
            elif response.status_code == 429:
                print(f"⏳ API Key {self.current_key} ติด Rate Limit! เปลี่ยนไปใช้ API Key อื่น...")
                self.current_key = next(self.api_keys)  # 🔁 เปลี่ยน API Key แล้วลองใหม่
            else:
                print(f"❌ เกิดข้อผิดพลาด: {response.status_code}")
                return None
