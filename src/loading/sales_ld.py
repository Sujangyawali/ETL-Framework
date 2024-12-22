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