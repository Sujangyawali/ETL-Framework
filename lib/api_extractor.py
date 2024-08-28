import requests

class APIExtractor:
    def __init__(self, api_url, api_key, headers, params):
        self.api_url = api_url
        self.api_key = api_key
        self.headers = headers
        self.params = params
        pass