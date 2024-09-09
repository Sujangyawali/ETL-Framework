import os
from lib.logger import Logger
from config.env_setup import *
from lib.api_extractor import APIExtractor


script_name=os.path.basename(__file__)
log = Logger(script_name)
log.log_message(f"Scripts for {script_name} has been started")


log.log_message(f"Initialising object for {script_name}")

API_KEY = API_KEY
API_HOST = API_HOST
ROOT_URL = ROOT_URL

headers = {
    'x-rapidapi-key' : API_KEY,
    'x-rapidapi-host' : API_HOST
}

params ={
    "category_id": "2478868012",
    "page": "1",
    "country": "US",
    "sort_by": "RELEVANCE",
    "product_condition": "ALL",
    "is_prime": "false"
}

end_point = 'products-by-category'
url = ROOT_URL + API_HOST + '/' + end_point
api_instance = APIExtractor(url, API_KEY, headers = headers, params = params, log = log)

log.log_message(f"Completed API object for initialization")


try:
    pass
except:
    pass
finally:
    log.close()
