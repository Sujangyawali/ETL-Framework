import os
from lib.logger import Logger
from config.env_setup import *
from lib.mysql_database_extractor import MySQLDataExtractor
from lib.utils import convert_to_csv, format_query
from lib.s3_operations import S3ObjectManager
from lib.snowflake import SnowflakeDatabase
from lib.script_execution_logger import ScriptExeLog


script_name = os.path.basename(__file__)
script_name = os.path.splitext(script_name)[0]
log = Logger(script_name)

DB_HOST = DB_HOST
DB_USER = DB_USER
DB_PASSWORD = DB_PASSWORD
DB_NAME = DB_NAME
DB_PORT = int(DB_PORT)
EXT_SRC_SCHEMA='RDW_DWH'
SOURCE_TABLE ='SALES_DATA'
SOURCE_TABLE_COLUMNS = ['TRANSACTION_ID','TRANSACTION_LINE_ID','STORE_ID','REGISTER_ID','EMPLOYEE_ID','CUSTOMER_ID','LOYALTY_CARD_NUMBER',
'ITEM_ID','ITEM_DESCRIPTION','QUANTITY','PRICE','DISCOUNT','TAX','TOTAL_AMOUNT','PAYMENT_METHOD','TRANSACTION_STATUS',
'RETURN_FLAG','PROMOTION_CODE','SALES_CHANNEL','RECEIPT_NUMBER','SALE_DATE','CURRENCY']
SF_LANDING_TABLE = 'LND_SALES'

db = MySQLDataExtractor(DB_HOST,DB_PORT, DB_USER, DB_PASSWORD,DB_NAME,log)
sf_object = SnowflakeDatabase(log)
sf_object.connect()

log.log_message("Database instance created")
script_exe_log_object = ScriptExeLog(sf_object, script_name, EXTRACTION_SCRIPT_TABLE, EXTRACTION_BATCH_LOG_TABLE)

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
        db.connect()
        log.log_message(f" Extracting data from  {EXT_SRC_SCHEMA}.{SOURCE_TABLE} Started...")
        EXTRACT_SQL = format_query(EXT_SRC_SCHEMA, SOURCE_TABLE, SOURCE_TABLE_COLUMNS )
        log.log_message(f"Extraction Query:\n {EXTRACT_SQL}")
        csv_file = db.extract_to_csv(EXTRACT_SQL, SOURCE_TABLE.lower(), chunk_size=100)
        file_name_on_bucket = os.path.basename(csv_file)
        if csv_file:
            s3_file_object = S3ObjectManager(log)
            s3_file_object.connect_s3()
            s3_file_object.upload_csv_to_s3_landing(csv_file, file_name_on_bucket)
            sf_object.load_s3_to_landing(file_name_on_bucket, SF_LANDING_TABLE)
        script_exe_log_object.update_success_status()
    except Exception as e:
        script_exe_log_object.update_error_status()
        raise Exception(f"[ERROR]: Error while extracting from MYsql Database.")
    finally:
        db.end_connection()
        sf_object.end_connection()
        s3_file_object.end_connection()
        log.close()

