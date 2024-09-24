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
            self.cursor = pymysql.connect(
                host = self.host,
                port = self.port, 
                user = self.user, 
                password = self.password,
                database = self.database,
                cursorclass = pymysql.cursors.DictCursor)
            self.log.log_message("Database connection started.")
        except pymysql.MySQLError as e:
            self.log.log_message("Unable to connect to MySQL database.")
            raise Exception(f"Error connecting to MySQL: {str(e)}")
    
    def get_data(self, query: str):
        self.query = query
        self.log.log_message(f"{self.query}")
        try:
            self.cursor.execute(self.query)
            self.log.log_message("Query executed.")
            result =  self.cursor.fetchall()
            self.log.log_message("Number of rows:" + str(self.db.rowcount))
            return result
        except pymysql.MySQLError as e:
            self.log.log_message("Error executing query.\n {e}")
    
    def end_connection(self):
        self.cursor.close()
        self.log.log_message("Database Session Closed")

