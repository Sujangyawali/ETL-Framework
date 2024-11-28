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
        SELECT 1 FROM {CONFIG_SCHEMA}.{EXTRACTION_SCRIPT_TABLE}
        WHERE SCRIPT_NAME = '{self.script_name}'
        """
        query_result = self.sf_db.execute_query(query).fetchone()
        return True if query_result else False
    def get_script_exe_status(self):
        "Get status of the script from log table"
        pass
    def insert_script_to_log(self):
        "Loads script in to log table with the status assigned to 'RNNING'"
        pass
    def insert_error_status(self):
        "Updates status of script to 'ERROR' in case of error"
        pass
    def insert_success_status(self):
        "Updates status of script to 'SUCCESS' in case of completion of script with out error"
        pass
    def check_running_status(self):
        "Cheks script status on log table if it is on the running state, aviod to re-running of running script"
        pass
    def check_error_status(self):
        "Cheks script status on log table if it has 'ERROR' status, aviod to re-running of running script"
        pass