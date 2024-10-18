from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 14),
    'email': ['your_email'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'amazon_el_pipeline', 
    default_args=default_args,
    concurrency=2, # limit to 2 tasks running at once
    catchup=False
)

# Dummy start and end tasks
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Parameters for MySQL to GCS extraction and GCS to BigQuery load
project_id = 'your_project_id'
bucket = 'your_bucket'
mysql_conn_id = 'your_mysql_conn_id'
gcp_conn_id = 'your_gcp_conn_id'
dataset_name = 'your_dataset_name'
file_ext = 'csv'
batch_size = 200000  # Number of rows per batch
max_rows_products = 1426337  # Total rows in amazon_products

# Function to create extract and load tasks
def create_extract_load_tasks(table, is_large_table=False):
    if is_large_table:
        # For large tables, perform batching
        for batch_number in range(0, (max_rows_products // batch_size) + 1):
            offset = batch_number * batch_size

            # Task to extract batch from MySQL to GCS
            extract_task = MySQLToGCSOperator(
                task_id=f'extract_{table}_batch_{batch_number}_to_gcs',
                mysql_conn_id=mysql_conn_id,
                sql=f'SELECT * FROM amazon.{table} LIMIT {batch_size} OFFSET {offset}',
                bucket=bucket,
                filename=f'{table}_batch_{batch_number}.{file_ext}',
                export_format=file_ext,
                schema_filename=f'{table}_schemas.json',
                gcp_conn_id=gcp_conn_id,
                dag=dag,
                execution_timeout=timedelta(minutes=30)
            )

            # Task to load batch from GCS to BigQuery
            load_task = GCSToBigQueryOperator(
                task_id=f'load_{table}_batch_{batch_number}_to_bigquery',
                bucket=bucket,
                source_objects=[f'{table}_batch_{batch_number}.{file_ext}'],
                destination_project_dataset_table=f'{project_id}:{dataset_name}.{table}',
                source_format=file_ext,
                skip_leading_rows=1,
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED',
                schema_object=f'{table}_schemas.json',
                dag=dag
            )

            extract_task >> load_task

    else:
        # For smaller tables, no batching needed
        extract_task = MySQLToGCSOperator(
            task_id=f'extract_{table}_to_gcs',
            mysql_conn_id=mysql_conn_id,
            sql=f'SELECT * FROM amazon.{table}',
            bucket=bucket,
            filename=f'{table}.{file_ext}',
            export_format=file_ext,
            schema_filename=f'{table}_schemas.json',
            gcp_conn_id=gcp_conn_id,
            dag=dag
        )

        load_task = GCSToBigQueryOperator(
            task_id=f'load_{table}_to_bigquery',
            bucket=bucket,
            source_objects=[f'{table}.{file_ext}'],
            destination_project_dataset_table=f'{project_id}:{dataset_name}.{table}',
            source_format=file_ext,
            skip_leading_rows=1,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            schema_object=f'{table}_schemas.json',
            dag=dag
        )

        extract_task >> load_task

# Create a TaskGroup for each table
tables = ['amazon_products', 'amazon_categories']

for table in tables:
    with TaskGroup(group_id=f'{table}_tasks', dag=dag) as tg:
        if table == 'amazon_products':
            create_extract_load_tasks(table, is_large_table=True)
        else:
            create_extract_load_tasks(table)
    
    start_task >> tg >> end_task
