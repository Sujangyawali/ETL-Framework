import snowflake.connector
from typing import Any, List, Tuple
from lib.logger import Logger
from config.env_setup import *
# need to import s3 helper module to eheck valid s3 file directory and file availability

class SnowflakeDatabase:
    def __init__(self, log: Logger):
        self.account = SF_ACCOUNT
        self.user = SF_USER
        self.password = SF_PASSWORD
        self.warehouse = SF_WAREHOUSE
        self.database = SF_DATABASE
        self.role = SF_ROLE
        self.log = log
    
    def connect(self):
        self.log.log_message(f"Snwoflake database connection started.")
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
            )
            self.cursor = self.connection.cursor()
            self.log.log_message(f"Snwoflake database connection established.")
            return 
        except Exception as e:
            self.log.log_message(f"Unable to connect to Snowflake database")
            raise Exception(f"Error connecting to Snowflake database {e}")
    
    def execute_query(self, query: str):
        self.log.log_message(f"Executing query: \n {query} ")
        try:
            self.query = query
            self.cursor.execute(f"USE ROLE {self.role}")
            self.cursor.execute(self.query)
            self.log.log_message(f"Query executed successfully.")
            self.log.log_message(f"Number of rows: {self.cursor.rowcount} .")
        except Exception as qe:
            self.log.log_message(f"Error executing query.\n {qe}")
            raise Exception(f"Error executing query: {qe}")
        
    def get_queried_data(self, query: str):
        self.log.log_message(f"Executing query: \n {query} ")
        try:
            self.query = query
            self.cursor.execute(f"USE ROLE {self.role}")
            self.cursor.execute(self.query)
            self.log.log_message(f"Query executed successfully.")
            self.log.log_message(f"Number of rows: {self.cursor.rowcount} .")
        except Exception as qe:
            self.log.log_message(f"Error executing query.\n {qe}")
            raise Exception(f"Error executing query: {qe}")
        return self.cursor.fetchall()
    
    def load_s3_to_landing(self, s3_file, landing_table):
        self.log.log_message(f"Loading data from S3 to landing table: {landing_table}")
        truncate_query = f"""
                            TRUNCATE TABLE {SF_LANDING_SCHEMA}.{landing_table}
                            """
        load_query = f"""
                    COPY INTO {SF_LANDING_SCHEMA}.{landing_table}
                    FROM @{SF_STAGE_SCHEMA}.{SF_STAGE}
                    FILES = ('{s3_file}')
                    """
        try:
            self.log.log_message("Truncating landing table")
            self.execute_query(truncate_query)
            self.log.log_message("Landing table truncated")
            self.log.log_message("Loading data into landing table")
            self.execute_query(load_query)
            self.log.log_message("Landing table loaded")
        except Exception as e:
            self.log.log_message(f"Error loading data from S3 to landing table: {e}")
            raise Exception(f"Error loading data from S3 to landing table: {e}")
        
    def end_connection(self):
        if self.connection:
            self.connection.close()
            self.log.log_message("Database Session Closed")

