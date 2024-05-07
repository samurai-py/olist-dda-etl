from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from airflow import Dataset
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

default_args = {
    "owner": "Mário Vasconcelos",
    "retries": 1,
    "retry_delay": 0
}

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    dag_id="load_local_files_dag",
    start_date=datetime(2024, 5, 5),
    schedule="@daily",
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['dev', 'etl', 'local', 'bq']
)

def load_local_files():

    schema = "olist"
    init_data_load = EmptyOperator(task_id="init")
    stop_data_load = EmptyOperator(task_id="finish")

    with TaskGroup(group_id="Local_Files") as local_files_task_group:

        local_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/raw_data/csv/*.csv',
        dst='raw/',
        bucket='olist-dw',
        gcp_conn_id='gcp_conn',
        mime_type='text/csv',
    )
        
    local_csv_to_gcs
    

    with TaskGroup(group_id="Load_to_bq") as load_to_bq_task_group:

        load_local_customers = aql.load_file(
            task_id="load_local_customers",
            input_file=File("gs://olist-dw/raw/olist_customers_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="customers", conn_id="gcp_conn")
        )

        load_local_customers

        load_local_orders = aql.load_file(
            task_id="load_local_orders",
            input_file=File("gs://olist-dw/raw/olist_orders_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="orders", conn_id="gcp_conn")
        )

        load_local_orders

        load_local_orders_payments = aql.load_file(
            task_id="load_local_orders_payments",
            input_file=File("gs://olist-dw/raw/olist_order_payments_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="payments", conn_id="gcp_conn")
        )

        load_local_orders_payments

        load_local_orders_items = aql.load_file(
            task_id="load_local_orders_items",
            input_file=File("gs://olist-dw/raw/olist_order_items_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="order_items", conn_id="gcp_conn")
        )

        load_local_orders_items

        load_local_orders_reviews = aql.load_file(
            task_id="load_local_orders_reviews",
            input_file=File("gs://olist-dw/raw/olist_order_reviews_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="reviews", conn_id="gcp_conn")
        )

        load_local_orders_reviews

        load_local_products = aql.load_file(
            task_id="load_local_products",
            input_file=File("gs://olist-dw/raw/olist_products_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="products", conn_id="gcp_conn")
        )

        load_local_products

        load_local_sellers = aql.load_file(
            task_id="load_local_sellers",
            input_file=File("gs://olist-dw/raw/olist_sellers_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="sellers", conn_id="gcp_conn")
        )

        load_local_sellers


        load_local_location = aql.load_file(
            task_id="load_local_location",
            input_file=File("gs://olist-dw/raw/olist_geolocation_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="location", conn_id="gcp_conn")
        )

        load_local_location

    with TaskGroup(group_id="Delete_files") as delete_files_task_group:

        delete_gcs_files_task = GoogleCloudStorageDeleteOperator(
        task_id='delete_gcs_files_task',
        bucket_name='olist-dw',
        prefix='raw/',
        gcp_conn_id='gcp_conn'
        )

    delete_gcs_files_task

    init_data_load >> local_files_task_group >> load_to_bq_task_group >> delete_files_task_group >> stop_data_load


dag = load_local_files()