import os

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago

JOB_ID = os.getenv("DATABRICKS_JOB_ID")

default_args = {
    "owner": "Mário Vasconcelos",
    "retries": 1,
    "retry_delay": 0
}


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    dag_id="process_data",
    start_date=days_ago(2),
    schedule_interval=None,
    max_active_runs=1,
    default_args=default_args,
    tags=['dev', 'etl', 'aws', 'databricks']
)

def run_etl_notebook():

    init_run = EmptyOperator(task_id="init")
    stop_run = EmptyOperator(task_id="finish")
    

    notebook_task = DatabricksRunNowOperator(
        task_id="run_etl_notebook",
        databricks_conn_id="databricks_conn",
        job_id=JOB_ID
    )

    init_run >> notebook_task >> stop_run

dag = run_etl_notebook()