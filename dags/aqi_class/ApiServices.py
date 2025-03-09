import time
import requests
import itertools
import logging

class ApiServices:
    def __init__(self, api_keys, rate_limit=5):
        self.api_keys = itertools.cycle(api_keys)  # ✅ ใช้ itertools.cycle เพื่อหมุน API Key
        self.rate_limit = rate_limit
        self.api_call_tracker = {key: [] for key in api_keys}  # ✅ เก็บ timestamp ของแต่ละ API Key
        self.current_key = next(self.api_keys)  # ✅ ตั้งค่า API Key ตัวแรก

    def _get_available_key(self):
        """✅ ตรวจสอบ API Key ที่ยังไม่ติด Rate Limit"""
        current_time = time.time()
        logging.info(f"🔄 Checking available API Key...")

        # ✅ รีเฟรช timestamp โดยลบค่าที่เกิน 60 วินาที
        for key in self.api_call_tracker:
            self.api_call_tracker[key] = [t for t in self.api_call_tracker[key] if current_time - t < 60]

        # ✅ เลือก API Key ที่ยังสามารถใช้งานได้
        for _ in range(len(self.api_call_tracker)):
            key = self.current_key  # ใช้ API Key ปัจจุบัน
            if len(self.api_call_tracker[key]) + 1 <= self.rate_limit:  # ✅ +1 API Call ที่กำลังจะเกิดขึ้นต้องถูกนับไปด้วย ทำให้ต้องเช็คว่า Key นี้จะยังอยู่ในโควต้าได้ไหม
                logging.info(f"✅ API Key {key} is available ({len(self.api_call_tracker[key])+1}/{self.rate_limit} calls used)")
                return key

            # ✅ หมุนไป API Key ถัดไป
            self.current_key = next(self.api_keys)

        # ✅ ถ้าไม่มี Key ใดใช้ได้ ต้องรอ 60 วินาที
        next_available_time = min(min(self.api_call_tracker.values(), key=lambda x: x[0]) or [current_time]) + 60
        wait_time = max(0, next_available_time - current_time)
        logging.warning(f"⏳ All API Keys reached limit! Waiting {wait_time:.2f} seconds...")
        time.sleep(wait_time)
        return self._get_available_key()  # ✅ ลองใหม่หลังจากรอ

    def fetch_api(self, full_path_url, params=None, max_retries=3):
        """✅ ดึงข้อมูล API และจัดการ Rate Limit"""
        retries = 0
        while retries < max_retries:
            current_key = self._get_available_key()
            params = params or {}
            params["key"] = current_key
            url = full_path_url

            try:
                response = requests.get(url, params=params)
                logging.info(f"🌐 API Request: {url} | Status: {response.status_code}")

                if response.status_code == 429:
                    logging.warning(f"⏳ API Key {current_key} reached limit! Switching API Key...")
                    self.current_key = next(self.api_keys)  # ✅ เปลี่ยน API Key ทันที
                    retries += 1
                    continue  # ✅ ลองใหม่กับ Key อื่น

                response.raise_for_status()
                self.api_call_tracker[current_key].append(time.time())  # ✅ บันทึก Timestamp API Key ที่ใช้

                return response.json()

            except requests.exceptions.RequestException as e:
                logging.error(f"❌ API Request failed: {e}")
                return None
        return None
