import os
from lib.logger import Logger
from config.env_setup import *
from lib.SFTP_extractor import SFTP


script_name = os.path.basename(__file__)
script_name = os.path.splitext(script_name)[0]
log = Logger(script_name)
log.log_message(f"Scripts for {script_name} has been started")


log.log_message(f"Initialising object for {script_name}")

SFTP_HOST = SFTP_HOST
SFTP_PORT = int(SFTP_PORT)
SFTP_USER = SFTP_USER
SFTP_PASSWORD = SFTP_PASSWORD
server_file_directory = '/home/blu/Cutover3/extraction/incoming/Store Closure'

sftpi = SFTP(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD, keyfile=None, log=log)
log.log_message(f"SFTP instance created")

try:
    log.log_message(f"Starting connection with SFTP server")
    sftpi.connect()
    log.log_message(f"Checking if the file is available...")
    file = sftpi.get_file(server_file_directory)
    process_flg = 0
    if file:
        log.log_message(f"File is available")
        log.log_message(f"File name: {file} ")
        process_flg = 1
    if process_flg == 1:
        log.log_message(f"File is available, processing the file...")
    if process_flg == 0:
        log.log_message(f"File is not available.Escaping the further process")
except Exception as e:
    raise Exception(f"[ERROR]: Error while extracting from SFTP server.")
finally:
    sftpi.close()
    log.log_message(f"Closed SFTP connection.")
    log.close()

