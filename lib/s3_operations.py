""" 
Module to interact with amazon s3 bucket to perfroms tasks that includes:
Upload file to S3 from data directory extrated by script from multiple sources like API, Database and SFTP server.
Fetch csv file from S3 to load into snowflake stage(landing) tables, we are considering S3 as secondary stage for Snowflake

"""
import boto3
from config.env_setup import *
from lib.logger import Logger

class S3ObjectManager:
    def __init__(self, bucket_name, access_key, secret_key, region_name, log: Logger):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name
        self.log = log
        
    def connect(self):
        self.log.log_message(f"Establishing connection to the S3 bucket.")
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name  
            )
            self.log.log_message(f"Connected to S3 bucekt: {self.bucket_name}.")
        except Exception as s3_e:
            self.log.log_message(f"Failed to connect to S3 bucket: {self.bucket_name}.")
            raise Exception(f"Failed to connect to S3 bucket: {self.bucket_name}\n {s3_e}")

    def upload_to_s3(self, file_name):
        self.log.log_message(f"Uploading of file: {file_name} to bucekt {self.bucket_name} bad been strated .")
        try:
            self.s3_client.upload_file(file_name, self.bucket_name)
            self.log.log_message(f"File uploaded to S3 bucket: {self.bucket_name} successfully")
        except:
            self.log.log_message(f"Failed to upload file to S3 bucket: {self.bucket_name}")
            raise Exception(f"Failed to upload file to S3 bucket: {self.bucket_name}")
        
    def end_connection(self):
        if self.s3_client:
            self.s3_client = None
            self.log.log_message(f"Closing the connection to the S3 bucket: {self.bucket_name}")
        
        