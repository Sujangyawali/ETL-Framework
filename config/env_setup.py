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
DB_HOST = 'DB_HOST'
DB_USER = 'DB_USER'
DB_PASSWORD = 'DB_PASSWORD'
DB_NAME = 'DB_NAME'
DB_PORT = 'DB_PORT'