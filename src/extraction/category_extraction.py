import os
from dotenv import load_dotenv
from lib.logger import Logger


load_dotenv()
API_KEY = os.getenv("API_KEY")
log = Logger("category_extraction")
log.log_message("Scripts for category has been started")
log.close
