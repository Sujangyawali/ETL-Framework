import pandas as pd
import datetime

def convert_to_csv(data:dict, csv_file_name:str, headers:list):
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    csv_file_name = csv_file_name + "-" + current_time + ".csv"
    # Convert data to pandas DataFrame and write to CSV
    df = pd.DataFrame(data, columns=headers)
    df.to_csv(csv_file_name, index=False)
    return csv_file_name
