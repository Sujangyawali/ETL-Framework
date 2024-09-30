""" 
MySQL Data Extraction Library

This module provides a `MySQLDataExtractor` class that allows for efficient extraction of data
from a MySQL database using predefined queries.
"""
import pymysql
import pymysql.cursors
from lib.logger import Logger

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
                cursorclass = pymysql.cursors.DictCursor)
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
    
    def end_connection(self):
        self.connection.close()
        self.log.log_message("Database Session Closed")
        

