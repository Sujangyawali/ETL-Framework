import os
from lib.logger import Logger
from config.env_setup import *
from lib.mysql_database_extractor import MySQLDataExtractor
from lib.utils import convert_to_csv


script_name = os.path.basename(__file__)
script_name = os.path.splitext(script_name)[0]
log = Logger(script_name)
log.log_message(f"Scripts for {script_name} has been started")


log.log_message(f"Initialising object for {script_name}")

DB_HOST = DB_HOST
DB_USER = DB_USER
DB_PASSWORD = DB_PASSWORD
DB_NAME = DB_NAME
DB_PORT = DB_PORT

EXT_SRC_SCHEMA='SCHEMA_NAME'
SOURCE_TABLE ='TABLE_NAME'

db = MySQLDataExtractor(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD,DB_NAME,log)
log.log_message("Database instance created")

try:
    db.connect()
    log.log_message(" Extracting data from  ${EXT_SRC_SCHEMA}.${SOURCE_TABLE} Started...")
except Exception as e:
    log.log_message(f"Something went wrong while API extraction:{e}")
    raise Exception(f"[ERROR]: Error while requesting REST API")
finally:
    log.close()
