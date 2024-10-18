""" 
MySQL Data Extraction Library

This module provides a `MySQLDataExtractor` class that allows for efficient extraction of data
from a MySQL database using predefined queries.
"""
import pymysql
import pymysql.cursors
from lib.logger import Logger
from config.env_setup import *
from datetime import datetime

class MySQLDataExtractor:
    def __init__(self, host, port, user, password, database, log: Logger):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.log = log

    def connect(self):
        self.log.log_message("Database connection started.")
        try:
            self.connection = pymysql.connect(
                host = self.host,
                port = self.port, 
                user = self.user, 
                password = self.password,
                database = self.database,
                cursorclass = pymysql.cursors.DictCursor
                 )
            self.cursor=self.connection.cursor()
            self.log.log_message("Database session started.")
        except Exception as e:
            self.log.log_message("Unable to connect to MySQL database.")
            raise Exception(f"Error connecting to MySQL: {e}")
    
    def get_data(self, query: str):
        self.query = query
        try:
            self.cursor.execute(f"USE  {self.database}")
            self.cursor.execute(self.query)
            self.log.log_message("Query executed.")
            result =  self.cursor.fetchall()
            self.log.log_message("Number of rows:" + str(self.cursor.rowcount))
            return result
        except Exception as e:
            self.log.log_message(f"Error executing query.\n {e}")
            raise Exception(f"Error executing query: {e}")
    
    def extract_to_csv(self, query: str, csv_file_name:str, chunk_size: int = 1000):
            try:
                self.cursor.execute(f"USE {self.database}")
                self.cursor.execute(query)
                total_rows_fetched = self.cursor.rowcount
                self.log.log_message(f"Query executed successfully. Total rows fetched: {total_rows_fetched}")
            except Exception as qe:
                qe = qe.args[1] if len(qe.args) > 1 else str(qe)
                self.log.log_message(f"Error executing query: {qe}")
                raise Exception(f"Error executing query: {qe}")
            csv_file_name = os.path.join(DATA_DIR,f"{csv_file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv")
            
            if total_rows_fetched != 0:
                self.log.log_message(f"Writing data to CSV file: {csv_file_name}")
                try:
                    with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
                        header = [col[0] for col in self.cursor.description]
                        file.write(','.join(header) + '\n')
                        while True:
                            data_chunk = self.cursor.fetchmany(chunk_size)

                            if not data_chunk:
                                break
                            
                            for row in data_chunk:
                                file.write(','.join(str(row[col]) for col in header) + '\n')
                        self.log.log_message(f"Data extraction to CSV completed.")
                except Exception as csve:
                    self.log.log_message(f"Error writing data to CSV file: {csve}")
    def end_connection(self):
        self.connection.close()
        self.log.log_message("Database Session Closed")
        

