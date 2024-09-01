import datetime
import os
from config.constants import *

class Logger:
    def __init__(self, script_name):
        current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%")
        log_path = os.getenv("LOG_PATH")
        log_file_name = str(script_name) + "_" + str(current_time) + ".log"
        log_file = os.path.join(log_path, log_file_name)
        self.log_file = open(log_file,'w')

    def log_message(self, msg, level = "INFO", boxed = True):
        now = datetime.datetime.now()
        log_level = level.upper()
        log_msg = f"[{now}]: {log_level} - {msg}\n"
        log_msg += "\n******************************************************************************************"
        self.log_file.write(log_msg)
        self.log_file.flush()
        
    def close(self):
        self.log_file.close()
