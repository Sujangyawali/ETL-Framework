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
        self.log.log_message(f"Starting extraction to CSV from query: {query}")
        try:
            self.cursor.execute(f"USE {self.database}")
            self.log.log_message(f"Query using database: {self.database}")
            csv_file_name = os.oath.join(DATA_DIR,f"{csv_file_name}_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv")
            self.log.log_message(f"Writing data to CSV file: {csv_file_name}")
            with open(csv_file_name, mode='w', newline='', encoding='utf-8') as file:
                header = [col[0] for col in self.cursor.description]
                file.write(','.join(header) + '\n')
                total_rows = 0 
                while True:
                    data_chunk = self.cursor.fetchmany(chunk_size)
                    if not data_chunk:
                        break
                    
                    for row in data_chunk:
                        file.write(','.join(str(row[col]) for col in header) + '\n')
                    total_rows += len(data_chunk)
                    self.log.log_message(f"Data extraction to CSV completed. Total rows written: {total_rows}")
        except Exception as e:
            self.log.log_message(f"Error during extraction to CSV: {e}")
            raise Exception(f"Error during extraction to CSV: {e}")











    def end_connection(self):
        self.connection.close()
        self.log.log_message("Database Session Closed")
        

