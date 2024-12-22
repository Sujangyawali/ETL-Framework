import os
from lib.logger import Logger
from config.env_setup import *
from lib.SFTP_extractor import SFTP
from lib.s3_operations import S3ObjectManager
from lib.snowflake import SnowflakeDatabase
from lib.script_execution_logger import ScriptExeLog


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
SF_LANDING_TABLE = 'LND_STORE_CLOSURE'

sftp_object = SFTP(SFTP_HOST, SFTP_PORT, SFTP_USER, SFTP_PASSWORD, keyfile=None, log=log)
log.log_message(f"SFTP instance created")

sf_object = SnowflakeDatabase(log)
sf_object.connect()
log.log_message("Snowflake Database instance created")
script_exe_log_object = ScriptExeLog(sf_object, script_name, EXTRACTION_SCRIPT_TABLE, EXTRACTION_BATCH_LOG_TABLE)

if not script_exe_log_object.is_script_audited():
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is not audited into {EXTRACTION_SCRIPT_TABLE} table, please audit the script first to run..")
if script_exe_log_object.get_script_exe_status() == 'RUNNING':
    sf_object.end_connection()
    log.close()
    raise Exception(f"Script is already in 'RUNNING' for Current Batch")
else:
    try:
        script_exe_log_object.remove_existing_log_for_current_batch()
        script_exe_log_object.insert_script_to_log()
        log.log_message(f"Starting connection with SFTP server")
        sftp_object.connect()
        log.log_message(f"Checking if the file is available...")
        file = sftp_object.check_file(server_file_directory)
        process_flg = 0
        if file:
            log.log_message(f"File is available")
            log.log_message(f"File name: {file} ")
            process_flg = 1
        if process_flg == 1:
            log.log_message(f"File is available, processing the file...")
            file_name_on_bucket = file
            server_file = server_file_directory + '/' + file
            csv_file = sftp_object.get_file(server_file)
            s3_file_object = S3ObjectManager(log)
            s3_file_object.connect_s3()
            s3_file_object.upload_csv_to_s3_landing(csv_file, file_name_on_bucket)
            sf_object.load_s3_to_landing(file_name_on_bucket, SF_LANDING_TABLE)
        if process_flg == 0:
            log.log_message(f"File is not available.Escaping the further process")
        script_exe_log_object.update_success_status()
    except Exception as e:
        script_exe_log_object.update_error_status()
        raise Exception(f"[ERROR]: Error while extracting from SFTP server.")
    finally:
        sftp_object.close()
        sf_object.end_connection()
        s3_file_object.end_connection()
        log.close()

