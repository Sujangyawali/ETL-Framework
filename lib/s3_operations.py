""" 
Module to interact with amazon s3 bucket to perfroms tasks that includes:
Upload file to S3 from data directory extrated by script from multiple sources like API, Database and SFTP server.
Fetch csv file from S3 to load into snowflake stage(landing) tables, we are considering S3 as secondary stage for Snowflake

"""
import boto3
from config.env_setup import *
from lib.logger import Logger

class S3ObjectManager:
    def __init__(self, bucket_name, access_key, secret_key, region_name):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.region_name = region_name
        self.s3 = boto3.client('s3', aws_access_key_id=self.access_key,
                               aws_secret_access_key=self.secret_key, region_name=self.region_name)
    
    def connect(self):
        pass

    def upload_to_s3(self):
        pass
    def end_connection(self):
        pass
        