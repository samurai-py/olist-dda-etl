from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "Mário Vasconcelos",
    "retries": 1,
    "retry_delay": 0
}

@dag(
    dag_id="dag_spark",
    start_date=datetime(2024, 5, 5),
    schedule=None,
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['dev', 'schema', 'local', 'bq']
)

def convert_files():

    csv_to_parquet = SparkSubmitOperator(
    task_id='convert_csv_to_parquet',
    application='/utils/csv_to_parquet.py',  # Caminho para o script PySpark
    name='Convert_CSV_to_Parquet',
    conn_id='spark_default',  # ID da conexão Spark no Airflow
    total_executor_cores=1,
    executor_cores=1,
    executor_memory='2g',
    num_executors=1,
    driver_memory='2g',
    verbose=True
)

    csv_to_parquet

dag = convert_files()