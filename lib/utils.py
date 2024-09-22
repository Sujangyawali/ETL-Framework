import pandas as pd
import datetime
from config.env_setup import *
from config.constants import *
import os

DATA_DIR = DATA_DIR
def convert_to_csv(data:dict, csv_file_name:str, headers:list):
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    csv_file_name = csv_file_name + "-" + current_time + ".csv"
    csv_file_name = os.path.join(DATA_DIR, csv_file_name)
    # Convert data to pandas DataFrame and write to CSV
    df = pd.DataFrame(data, columns=headers)
    df.to_csv(csv_file_name, index=False)
    return csv_file_name
