import airflow
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable

import datetime


''' 
============================================================================= 
== Define DAG schedule
============================================================================= 
''' 
schedule = None

''' 
============================================================================= 
== Read config from airflow variable 
============================================================================= 
''' 

config = Variable.get("config")

dag_config = Variable.get("extraction_config", deserialize_json=True)

ssh_cmd = dag_config["sales_extraction"].format(config=config)


''' 
============================================================================= 
== Define DAG , Tasks and dependencies.. 
============================================================================= 
''' 
with airflow.DAG(
     dag_id = "sales_extraction",
     start_date=datetime.datetime(2024, 12, 25),
     schedule = schedule,
     catchup = False
	) as dag:

    start = EmptyOperator(task_id='start')

    sales_extract  = SSHOperator(
         task_id = 'sales_extraction', 
         ssh_conn_id='ssh_ec2',
         command=ssh_cmd,
         dag=dag,
         cmd_timeout = None
        )
    
    end = EmptyOperator(task_id='end')

    start >> sales_extract >> end

    
