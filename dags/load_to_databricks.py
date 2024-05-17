from datetime import datetime

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

default_args = {
    "owner": "Mário Vasconcelos",
    "retries": 1,
    "retry_delay": 0
}


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    dag_id="load_to_databricks",
    start_date=datetime(2024, 5, 5),
    schedule="@daily",
    max_active_runs=1,
    default_args=default_args,
    catchup=False,
    tags=['dev', 'etl', 'local', 'databricks']
)

def load_files_to_databricks():

    schema = "default"
    init_data_load = EmptyOperator(task_id="init")
    stop_data_load = EmptyOperator(task_id="finish")
    

    with TaskGroup(group_id="Customers") as customers_task_group:

        load_local_customers = aql.load_file(
            task_id="load_local_customers",
            input_file=File("include/raw_data/csv/olist_customers_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="customers", conn_id="databricks_conn")
        )

    load_local_customers


    with TaskGroup(group_id="Orders") as orders_task_group:

        load_local_orders = aql.load_file(
            task_id="load_local_orders",
            input_file=File("include/raw_data/csv/olist_orders_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="orders", conn_id="databricks_conn")
        )

    load_local_orders

    with TaskGroup(group_id="Orders_payments") as orders_payments_task_group:

        load_local_orders_payments = aql.load_file(
            task_id="load_local_orders_payments",
            input_file=File("include/raw_data/csv/olist_order_payments_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="payments", conn_id="databricks_conn")
        )

    load_local_orders_payments

    with TaskGroup(group_id="Orders_items") as orders_items_task_group:

        load_local_orders_items = aql.load_file(
            task_id="load_local_orders_items",
            input_file=File("include/raw_data/csv/olist_order_items_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="order_items", conn_id="databricks_conn")
        )

    load_local_orders_items

    with TaskGroup(group_id="Orders_reviews") as orders_reviews_task_group:

        load_local_orders_reviews = aql.load_file(
            task_id="load_local_orders_reviews",
            input_file=File("include/raw_data/csv/olist_order_reviews_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="reviews", conn_id="databricks_conn")
        )

    load_local_orders_reviews

    with TaskGroup(group_id="Products") as products_task_group:

        load_local_products = aql.load_file(
            task_id="load_local_products",
            input_file=File("include/raw_data/csv/olist_products_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="products", conn_id="databricks_conn")
        )

    load_local_products

    with TaskGroup(group_id="Sellers") as sellers_task_group:

        load_local_sellers = aql.load_file(
            task_id="load_local_sellers",
            input_file=File("include/raw_data/csv/olist_sellers_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="sellers", conn_id="databricks_conn")
        )

    load_local_sellers

    with TaskGroup(group_id="Location") as location_task_group:

        load_local_location = aql.load_file(
            task_id="load_local_location",
            input_file=File("include/raw_data/csv/olist_geolocation_dataset.csv"),
            output_table=Table(metadata=Metadata(schema=schema), name="location", conn_id="databricks_conn")
        )

    load_local_location

    init_data_load >> [customers_task_group, 
                       orders_task_group, 
                       orders_items_task_group, 
                       orders_payments_task_group, 
                       orders_reviews_task_group, 
                       products_task_group, 
                       sellers_task_group,
                       location_task_group] >> stop_data_load

dag = load_files_to_databricks()