import airflow
from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.helpers import chain
import datetime

''' 
============================================================================= 
== Master DAG to trigger extraction & load DAGs
============================================================================= 
''' 

email_recepient_list = Variable.get('email_receiptients_list').split(",")

default_args = {
				'depends_on_past': False,
				'email': email_recepient_list,
                'email_on_failure' : True,
                'email_on_retry':True
}

schedule= "30 0 * * *"

''' 
============================================================================= 
== Define DAG , Tasks and dependencies.. 
============================================================================= 
''' 

with airflow.DAG(
     dag_id = "extraction_load_master",
     start_date=datetime.datetime(2024, 12, 25),
     schedule = schedule,
     max_active_tasks= 8,
     default_args=default_args,
     catchup = False
	) as dag:

    start = EmptyOperator(task_id='start')

    trigger_category_extraction = TriggerDagRunOperator(
        task_id="trigger_category_extraction",
        trigger_dag_id="category_extraction",
        wait_for_completion=True
    )

    trigger_store_closure_extraction = TriggerDagRunOperator(
        task_id="trigger_store_closure_extraction",
        trigger_dag_id="store_closure_extraction",
        wait_for_completion=True
    )

    trigger_sales_extraction = TriggerDagRunOperator(
        task_id="trigger_sales_extraction",
        trigger_dag_id="sales_extraction",
        wait_for_completion=True
    )

    trigger_category_load = TriggerDagRunOperator(
        task_id="trigger_category_load",
        trigger_dag_id="category_load",
        wait_for_completion=True
    )

    trigger_store_closure_load = TriggerDagRunOperator(
        task_id="trigger_store_closure_load",
        trigger_dag_id="store_closure_load",
        wait_for_completion=True
    )

    trigger_sales_load = TriggerDagRunOperator(
        task_id="trigger_sales_load",
        trigger_dag_id="sales_load",
        wait_for_completion=True
    )

    end = EmptyOperator(task_id='end')

    start >> [trigger_category_extraction, trigger_store_closure_extraction, trigger_sales_extraction] >> end

    trigger_category_extraction >> trigger_category_load >> end

    trigger_store_closure_extraction >> trigger_store_closure_load >> end

    [trigger_category_load, trigger_store_closure_load, trigger_sales_extraction] >> trigger_sales_load >> end

    # we can also use helper function for the complex dag dependencies