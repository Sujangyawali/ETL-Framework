import os
from dotenv import load_dotenv

load_dotenv()

#API ENV VARIABLES
API_KEY = os.getenv('API_KEY')
API_HOST = os.getenv('API_HOST')
LOG_PATH = os.getenv('LOG_PATH')
ROOT_URL = os.getenv('ROOT_URL')

#MYSQL DATABSE ENV VARIABLES
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')

#SFTP VARIABLES
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = os.getenv('SFTP_PORT')
SFTP_USER = os.getenv('SFTP_USER')
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD')


#ENVIRONMENT SETTINGS
DATA_DIR = os.getenv('DATA_DIR')

#SNOWFLAKE DATABSE VARIABLE
SF_ACCOUNT = os.getenv('SF_ACCOUNT')
SF_USER = os.getenv('SF_USER')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_WAREHOUSE = os.getenv('SF_WAREHOUSE')
SF_DATABASE = os.getenv('SF_DATABASE')
SF_ROLE = os.getenv('SF_ROLE')

#SNOWFLAKE STAGE
SF_STAGE = os.getenv('SF_STAGE')

#SNOWFLAKE STANDARD TABLE AND SCHEMA
SF_STAGE_SCHEMA = os.getenv('SF_STAGE_SCHEMA')
SF_LANDING_SCHEMA = os.getenv('SF_LANDING_SCHEMA')
CONFIG_SCHEMA = os.getenv('CONFIG_SCHEMA')
STAGE_VIEW_SCHEMA = os.getenv('STAGE_VIEW_SCHEMA')
TEMP_VIEW_SCHEMA = os.getenv('TEMP_VIEW_SCHEMA')
TEMP_SCHEMA = os.getenv('TEMP_SCHEMA')
TARGET_SCHEMA = os.getenv('TARGET_SCHEMA')
BATCH_DATE_TABLE = os.getenv('BATCH_DATE_TABLE')
EXTRACTION_SCRIPT_TABLE = os.getenv('EXTRACTION_SCRIPT_TABLE')
EXTRACTION_BATCH_LOG_TABLE = os.getenv('EXTRACTION_BATCH_LOG_TABLE')
LOAD_SCRIPT_TABLE = os.getenv('LOAD_SCRIPT_TABLE')
LOAD_BATCH_LOG_TABLE = os.getenv('LOAD_BATCH_LOG_TABLE')

#AWS BATCH USER CREDENTIALS
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_S3_BUCKET_REGION = os.getenv('AWS_S3_BUCKET_REGION')
AWS_S3_LANDING_BUCKET = os.getenv('AWS_S3_LANDING_BUCKET')

