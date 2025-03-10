import itertools
import time
import requests

class ApiServices:

    def __init__(self, keys:list):
        self.keys = keys
        self.api_keys = itertools.cycle(self.keys)
        self.memory = {key: 0 for key in self.keys}
        self.current_key = next(self.api_keys)

    def mask_keys(self,memory:dict,show_digit=2):
        return {key[:show_digit] + "*******" + key[-show_digit:]: value for key, value in memory.items()}

    
    def mask_key(self,secret:str,show_digit=2):
        return str(secret[:show_digit] + "*******" + secret[-show_digit:])
    
    def reset_usage(self,memory):
        for key in memory:
            memory[key] = 0

    def update_usage(self,memory,key):
        memory[key] += 1
    
    def check_usage(self,memory,limit=5,wait_time=60):
        if all(value >= limit for value in memory.values()):
            print(f"All API keys reached the limit. Waiting {wait_time} seconds...")
            time.sleep(wait_time)  # ✅ Wait for 60 seconds
            self.reset_usage(memory)

        self.current_key = next(self.api_keys)
        print(self.mask_key(self.current_key))
        return self.current_key
        

    def fetch_api(self, url:str ,params:dict = None):
        print(f"🌐 ใช้ API Key: {self.mask_key(self.current_key)} | Request: {url}")
        params["key"] = self.current_key
        response = requests.get(url, params)

        if response.status_code == 200:
            print(f"✅ สำเร็จ! Data: {response.json()}")
            self.update_usage(self.memory,self.current_key)
            print(self.mask_keys(self.memory))
            self.check_usage(self.memory,5)
            return response.json()
        elif response.status_code == 400:
            print("❌ ไม่สำเร็จ 400")
            self.update_usage(self.memory,self.current_key)
            print(self.mask_keys(self.memory))
            self.check_usage(self.memory,5)
            return None
        elif response.status_code == 429:
            print("⛔ ไม่สำเร็จ 429")
            self.update_usage(self.memory,self.current_key)
            print(self.mask_keys(self.memory))
            self.check_usage(self.memory,5)
            return None
        
        return None
