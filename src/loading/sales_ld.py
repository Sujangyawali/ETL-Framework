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

sf_object = SnowflakeDatabase(log)
sf_object.connect()

script_exe_log_object = ScriptExeLog(sf_object, script_name)

if not script_exe_log_object.is_script_audited():
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is not audited into {EXTRACTION_SCRIPT_TABLE} table, please audit the script first to run..")
if script_exe_log_object.get_script_exe_status() == 'RUNNING':
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is already in 'RUNNING' for Current Batch")
