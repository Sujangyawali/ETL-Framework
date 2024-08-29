import datetime
import os

class Logger:
    def __init__(self, script_name):
        current_time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")
        log_path = os.getenv("LOG_PATH")
        log_file_name = str(script_name) + "_" +str(current_time) + ".log"
        log_file = os.path.join(log_path, log_file_name)