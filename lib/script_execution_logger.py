from config.env_setup import *
from lib.snowflake import SnowflakeDatabase
class ScriptExeLog:
    def __init__(self, sf_db: SnowflakeDatabase, script_name, landing_table):
        self.script_name = script_name
        self.landing_table = landing_table
        self.sf_db = sf_db
        self.batch_date, self.batch_id = self.get_batch_info()

    def get_batch_info(self):
        "To get batch date and Batch ID variable"
        query = f"""
                    SELECT BATCH_DATE, BATCH_ID FROM {CONFIG_SCHEMA}.{BATCH_DATE_TABLE}
                """
        query_result = self.sf_db.execute_query(query).fetchone()
        batch_date, batch_id = query_result[0], query_result[1]
        return batch_date, batch_id
    
    def is_script_audited(self):
        "Checks if script presents on the scaript table if not the script must be audited on the table to run"
        query = f"""
        SELECT SCRIPT_ID  FROM {CONFIG_SCHEMA}.{EXTRACTION_SCRIPT_TABLE}
        WHERE SCRIPT_NAME = '{self.script_name}'
        """
        query_result = self.sf_db.execute_query(query).fetchone()
        if query_result:
            self.script_id = query_result[0]
        return True if query_result else False
    def get_script_exe_status(self):
        "Get status of the script from log table"
        self.get_batch_info()
        query = f"""
                    SELECT STATUS FROM {CONFIG_SCHEMA}.{EXTRACTION_BATCH_LOG_TABLE} WHERE JOB_NAME = '{self.script_name} AND BATCH_DATE = '{self.batch_date}'
                """
        query_result = self.sf_db.execute_query(query).fetchone()
        self.status = query_result[0]
        return self.status
    def insert_script_to_log(self):
        "Loads script in to log table with the status assigned to 'RNNING'"
        query = f"""
                    INSERT INTO {CONFIG_SCHEMA}.{EXTRACTION_BATCH_LOG_TABLE} (BATCH_DATE,SCRIPT_ID,STATUS)
                    VALUES ({self.batch_date},{self.script_id},'RUNNING')

                """
        self.sf_db.execute_query(query)
    def update_error_status(self):
        "Updates status of script to 'ERROR' in case of error"
        query = f"""
                    UPDATE {CONFIG_SCHEMA}.{EXTRACTION_BATCH_LOG_TABLE} SET STATUS = 'ERROR' , END_TIME = CURRENT_TIMESTAMP()
                    WHERE SCRIPT_ID = {self.script_id} and BATCH_DATE = {self.batch_date}

                """
        self.sf_db.execute_query(query)
    def update_success_status(self):
        "Updates status of script to 'SUCCESS' in case of completion of script with out error"
        query = f"""
                    UPDATE {CONFIG_SCHEMA}.{EXTRACTION_BATCH_LOG_TABLE} SET STATUS = 'SUCCESS' , END_TIME = CURRENT_TIMESTAMP()
                    WHERE SCRIPT_ID = {self.script_id} and BATCH_DATE = {self.batch_date}

                """
        self.sf_db.execute_query(query)
    