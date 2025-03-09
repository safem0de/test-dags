import time
import requests
import itertools
import logging

class ApiServices:
    def __init__(self, api_keys, rate_limit=5):
        self.api_keys = itertools.cycle(api_keys)  # ✅ ใช้ itertools.cycle เพื่อหมุน API Key
        self.rate_limit = rate_limit
        self.api_call_tracker = {key: [] for key in api_keys}  # ✅ เก็บ timestamp ของแต่ละ API Key
        self.current_key = next(self.api_keys)  # ✅ เริ่มต้นที่ Key แรก

    def _get_available_key(self):
        """✅ ตรวจสอบว่า API Key ไหนพร้อมใช้งาน"""
        current_time = time.time()
        logging.info(f"🔄 Checking available API Key...")

        for key in self.api_call_tracker:
            timestamps = self.api_call_tracker[key]
            self.api_call_tracker[key] = [t for t in timestamps if current_time - t < 60]  # ✅ ลบ timestamp ที่เกิน 60 วิ

            if len(self.api_call_tracker[key]) < self.rate_limit:
                logging.info(f"✅ API Key {key} is available ({len(self.api_call_tracker[key])}/{self.rate_limit} calls used)")
                return key  # ✅ คืนค่า Key ที่ยังมีโควต้าใช้งาน

        # ✅ ถ้าไม่มี Key ใดว่าง ต้องรอให้ครบ 60 วิ
        wait_time = 60 - (current_time - min(min(self.api_call_tracker.values(), key=lambda x: x[0]) or [0]))
        logging.warning(f"⏳ All API Keys reached limit! Waiting {wait_time:.2f} seconds...")
        time.sleep(wait_time)
        return self._get_available_key()  # ✅ ลองใหม่หลังจากรอ

    def fetch_api(self, full_path_url, params=None, max_retries=3):
        """✅ ดึงข้อมูล API และรองรับกรณี Too Many Requests (429)"""
        retries = 0
        while retries < max_retries:
            self.current_key = self._get_available_key()
            params = params or {}
            params["key"] = self.current_key
            url = full_path_url

            try:
                response = requests.get(url, params=params)
                logging.info(f"🌐 API Request: {url} | Status: {response.status_code}")

                if response.status_code == 429:
                    wait_time = 60  # ✅ รอ 60 วินาที ถ้าโดน Rate Limit
                    logging.warning(f"⏳ API Key {self.current_key} reached limit! Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    retries += 1
                    continue  # ✅ ลองใหม่หลังจากรอ

                response.raise_for_status()
                self.api_call_tracker[self.current_key].append(time.time())  # ✅ บันทึก Timestamp API Key

                return response.json()

            except requests.exceptions.RequestException as e:
                logging.error(f"❌ API Request failed: {e}")
                return None
        return None
