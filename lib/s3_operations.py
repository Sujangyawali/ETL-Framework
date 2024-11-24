""" 
Module to interact with amazon s3 bucket to perfroms tasks that includes:
Upload file to S3 from data directory extrated by script from multiple sources like API, Database and SFTP server.
Fetch csv file from S3 to load into snowflake stage(landing) tables, we are considering S3 as secondary stage for Snowflake

"""
import boto3
from config.env_setup import *
from lib.logger import Logger

class S3ObjectManager:
    def __init__(self,log: Logger):
        self.access_key = AWS_ACCESS_KEY_ID
        self.secret_key = AWS_SECRET_ACCESS_KEY
        self.region_name = AWS_S3_BUCKET_REGION
        self.log = log
        
    def connect_s3(self):
        self.log.log_message(f"Establishing connection to the S3 bucket.")
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name  
            )
            self.log.log_message(f"Connected to S3.")
        except Exception as s3_e:
            self.log.log_message(f"Failed to connect to S3.")
            raise Exception(f"Failed to connect to S3\n {s3_e}")

    def upload_csv_to_s3_landing(self, file_name):
        landing_bucket = AWS_S3_LANDING_BUCKET
        self.log.log_message(f"Uploading of file: {file_name} to bucekt {landing_bucket}.")
        try:
            self.s3_client.upload_file(file_name, landing_bucket,file_name)
            self.log.log_message(f"File uploaded to S3 bucket: {landing_bucket} successfully")
        except Exception as upld_e:
            self.log.log_message(f"Failed to upload file to S3 bucket: {landing_bucket}")
            raise Exception(f"Failed to upload file to S3 bucket: {landing_bucket}\n {upld_e}")
        
    def end_connection(self):
        if self.s3_client:
            self.s3_client = None
            self.log.log_message(f"Closing the connection to the S3 bucket.")
        
        