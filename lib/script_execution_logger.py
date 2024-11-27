
class ScriptExeLog:
    def __init__(self, log_table_schema, script_name, landing_table):
        self.log_table_schema = log_table_schema
        self.script_name = script_name
        self.landing_table = landing_table

    def get_batch_info(self):
        "To get batch date and Batch ID variable"
        pass
    
    def is_script_audited(self):
        "Checks if script presents on the scaript table if not the script must be audited on the table to run"
        pass
    def get_script_info(self):
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