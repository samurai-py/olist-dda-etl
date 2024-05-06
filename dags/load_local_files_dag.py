from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from airflow import Dataset
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


dbt_dataset = Dataset("dbt_load")


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
        src='include/raw_data/*.csv',
        dst='raw/',
        bucket='olist-dw',
        gcp_conn_id='gcp_conn',
        mime_type='text/csv',
    )
        
    local_csv_to_gcs
    

    with TaskGroup(group_id="Customers") as customers_task_group:

        load_local_customers = aql.load_file(
            task_id="load_local_customers",
            input_file=File("gs://olist-dw/raw/olist_customers_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="customers", conn_id="gcp_conn")
        )

    load_local_customers


    with TaskGroup(group_id="Orders") as orders_task_group:

        load_local_orders = aql.load_file(
            task_id="load_local_orders",
            input_file=File("gs://olist-dw/raw/olist_orders_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="orders", conn_id="gcp_conn")
        )

    load_local_orders

    with TaskGroup(group_id="Orders_payments") as orders_payments_task_group:

        load_local_orders_payments = aql.load_file(
            task_id="load_local_orders_payments",
            input_file=File("gs://olist-dw/raw/olist_order_payments_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="payments", conn_id="gcp_conn")
        )

    load_local_orders_payments

    with TaskGroup(group_id="Orders_items") as orders_items_task_group:

        load_local_orders_items = aql.load_file(
            task_id="load_local_orders_items",
            input_file=File("gs://olist-dw/raw/olist_order_items_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="order_items", conn_id="gcp_conn")
        )

    load_local_orders_items

    with TaskGroup(group_id="Orders_reviews") as orders_reviews_task_group:

        load_local_orders_reviews = aql.load_file(
            task_id="load_local_orders_reviews",
            input_file=File("gs://olist-dw/raw/olist_order_reviews_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="reviews", conn_id="gcp_conn")
        )

    load_local_orders_reviews

    with TaskGroup(group_id="Products") as products_task_group:

        load_local_products = aql.load_file(
            task_id="load_local_products",
            input_file=File("gs://olist-dw/raw/olist_products_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="products", conn_id="gcp_conn")
        )

    load_local_products

    with TaskGroup(group_id="Sellers") as sellers_task_group:

        load_local_sellers = aql.load_file(
            task_id="load_local_sellers",
            input_file=File("gs://olist-dw/raw/olist_sellers_dataset.csv", conn_id="gcp_conn", filetype=FileType.CSV),
            output_table=Table(metadata=Metadata(schema=schema), name="sellers", conn_id="gcp_conn")
        )

    load_local_sellers

    init_data_load >> local_files_task_group >> [customers_task_group, 
                       orders_task_group, 
                       orders_items_task_group, 
                       orders_payments_task_group, 
                       orders_reviews_task_group, 
                       products_task_group, 
                       sellers_task_group] >> stop_data_load


dag = load_local_files()