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
        try:
            self.log.log_message(f"Sending GET request to {self.api_url}.\n Parameters: {self.params}")
            response = requests.get(self.api_url, headers=self.headers, params=self.params)
            if response.status_code == 200:
                self.log.log_message(f"API request successful.")
                responsed_dict = response.json()
                return responsed_dict
            else:
                self.log.log_message(f"API request failed with status code {response.status_code}\n.Messages:{response.json()['messages']}")
        except Exception as e:
            self.log.log_message(f"An error occured {e}")
            raise Exception(f"[ERROR]: Error while requesting REST API: \n {e}")
        