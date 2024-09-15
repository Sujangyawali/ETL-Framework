import os
from lib.logger import Logger
from config.env_setup import *
from lib.api_extractor import APIExtractor
from lib.utils import convert_to_csv


script_name = os.path.basename(__file__)
script_name = os.path.splitext(script_name)[0]
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
csv_file_headers = ['asin', 'product_title', 'product_price']
csv_file_name = 'products_by_category'

log.log_message(f"Completed API object for initialization")

try:
    log.log_message(f"Ready for API request")
    responsed_dict = api_instance.api_request()
    # print(responsed_dict)
    if responsed_dict:
        log.log_message(f"Received JSON response")
    else:
        log.log_message(f"No expected JSON response received")
    products = responsed_dict['data']['products']
    log.log_message("Generation of CSV file is started")
    generated_csv_file = convert_to_csv(products, csv_file_name, csv_file_headers)
    log.log_message(f"CSV file {generated_csv_file} has been generated")
except Exception as e:
    log.log_message(f"Something went wrong while API extraction:{e}")
    raise Exception(f"[ERROR]: Error while requesting REST API")
finally:
    log.close()
