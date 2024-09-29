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

def format_query(schema_name:str, table_name:str, column_list:list, where_condition:str = None):
    """
    Returns query based on the provided information.
    Formats a SQL query based on the provided schema, table, columns, and optional WHERE condition.

    Args:
        schema_name (str): The name of the database schema.
        table_name (str): The name of the table to query.
        column_list (list): A list of column names to select in the query.
        where_condition (str, optional): An optional WHERE clause condition in SQL syntax.
        Example format: "column1 = 'value1' AND column2 > 'value2'".
        If no condition is provided, the WHERE clause will be omitted.

    Returns:
        str: A formatted SQL query string.

    Example:
        >>> format_query('my_schema', 'employees', ['id', 'name', 'salary'], "age > 30")
        "SELECT id, name, salary FROM my_schema.employees WHERE age > 30"

    """
    columns = ', '.join(column_list)
    query = f"SELECT {columns} FROM {schema_name}.{table_name}"
    if where_condition:
        query += f" WHERE {where_condition}"
    query += ";"
    return query