import requests
from lib.logger import Logger

class APIExtractor:
    def __init__(self, api_url, api_key, headers, params, log : Logger):
        self.api_url = api_url
        self.api_key = api_key
        self.headers = headers
        self.params = params
        self.log = log
    
    def api_request(self):
        self.log.log_message(f"Sending GET request to {self.api_url} with params: {self.params}")
        response = requests.get(self.api_url, headers=self.headers, params=self.params)
        