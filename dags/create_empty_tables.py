from datetime import datetime

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator


default_args = {
    "owner": "Mário Vasconcelos",
    "retries": 1,
    "retry_delay": 0
}

@dag(
    dag_id="create_empty_tables",
    start_date=datetime(2024, 5, 5),
    schedule=None,
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['dev', 'schema', 'local', 'bq']
)

def create_schema():

    create_schema_bq = BigQueryCreateEmptyDatasetOperator(
            task_id='create_schema_bq',
            dataset_id='olist',
            gcp_conn_id='gcp_conn',
        )
    
    create_schema_bq

dag = create_schema()