import os
from dotenv import load_dotenv

load_dotenv()

#API ENV VARIABLES
API_KEY = os.getenv('API_KEY')
API_HOST = os.getenv('API_HOST')
LOG_PATH = os.getenv('LOG_PATH')
ROOT_URL = os.getenv('ROOT_URL')
ROOT_URL = os.getenv('DATA_DIR')

#DATABSE ENV VARIABLES
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