import time
import requests
import itertools

class ApiServices:
    def __init__(self, api_keys, rate_limit=5):
        self.api_keys = itertools.cycle(api_keys)               # ✅ ใช้ itertools.cycle เพื่อหมุน Key
        self.rate_limit = rate_limit
        self.api_call_tracker = {key: [] for key in api_keys}   # ✅ เก็บ timestamp ของแต่ละ API Key
        self.current_key = next(self.api_keys)                  # ✅ เริ่มต้นที่ Key แรก

    def _get_available_key(self):
        """✅ ตรวจสอบว่า API Key ไหนพร้อมใช้งาน"""
        current_time = time.time()

        for key in self.api_call_tracker:
            timestamps = self.api_call_tracker[key]
            timestamps = [t for t in timestamps if current_time - t < 60]  # ✅ ลบค่าเก่าที่เกิน 60 วินาที
            self.api_call_tracker[key] = timestamps

            if len(timestamps) < self.rate_limit:
                return key

        # ✅ ถ้าไม่มี Key ใดว่าง ต้องรอให้ครบ 60 วินาที
        oldest_timestamp = min(
            min(self.api_call_tracker.values(), key=lambda x: x[0]) or [current_time]
        )
        wait_time = max(60 - (current_time - oldest_timestamp), 0)
        print(f"⏳ All API Keys reached limit! Waiting {wait_time:.2f} seconds...")
        time.sleep(wait_time)
        return self._get_available_key()  # ✅ ลองใหม่หลังจากรอ

    def fetch_api(self, full_path_url, params=None):
        """✅ ดึงข้อมูล API โดยเลือก Key ที่พร้อมใช้งาน"""
        self.current_key = self._get_available_key()
        params = params or {}
        params["key"] = self.current_key  # ✅ ใช้ API Key ที่เลือก
        url = full_path_url

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()

            # ✅ บันทึก Timestamp ของ API Key ที่ใช้งาน
            self.api_call_tracker[self.current_key].append(time.time())

            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ API Request failed: {e}")
            return None
