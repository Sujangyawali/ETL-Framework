import os
from lib.logger import Logger
from config.env_setup import *
from lib.api_extractor import APIExtractor
from lib.utils import convert_to_csv
from lib.snowflake import SnowflakeDatabase
from lib.script_execution_logger import ScriptExeLog
from lib.s3_operations import S3ObjectManager


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
csv_file_headers = ['asin', 'product_title', 'product_price']
csv_file_name = 'products_by_category'
url = ROOT_URL + API_HOST + '/' + end_point
SF_LANDING_TABLE = 'LND_PRDCT_CATGRY'

api_instance = APIExtractor(url, API_KEY, headers = headers, params = params, log = log)
sf_object = SnowflakeDatabase(log)
sf_object.connect()
log.log_message(f"Completed API object for initialization")
script_exe_log_object = ScriptExeLog(sf_object, script_name)


if not script_exe_log_object.is_script_audited():
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is not audited into {EXTRACTION_SCRIPT_TABLE} table, please audit the script first to run..")
if script_exe_log_object.get_script_exe_status() == 'RUNNING':
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is already in 'RUNNING' for Current Batch")
else:
    try:
        script_exe_log_object.remove_existing_log_for_current_batch()
        script_exe_log_object.insert_script_to_log()
        log.log_message(f"Ready for API request")
        responsed_dict = api_instance.api_request()
        if responsed_dict:
            log.log_message(f"Received JSON response")
            products = responsed_dict['data']['products']
            log.log_message("Generation of CSV file is started")
            csv_file = convert_to_csv(products, csv_file_name, csv_file_headers)
            log.log_message(f"CSV file {csv_file} has been generated")
            file_name_on_bucket = os.path.basename(csv_file)
            s3_file_object = S3ObjectManager(log)
            s3_file_object.connect_s3()
            s3_file_object.upload_csv_to_s3_landing(csv_file, file_name_on_bucket)
            sf_object.load_s3_to_landing(file_name_on_bucket, SF_LANDING_TABLE)
        else:
            log.log_message(f"No expected JSON response received")
        script_exe_log_object.update_success_status()
    except Exception as e:
        script_exe_log_object.update_error_status()
        log.log_message(f"Something went wrong while API extraction \n {e}")
        raise Exception(f"[ERROR]: Error while requesting REST API: \n {e}")
    finally:
        sf_object.end_connection()
        s3_file_object.end_connection()
        log.close()
