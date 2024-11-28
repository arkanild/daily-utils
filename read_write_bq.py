from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
import requests
import json
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'api_to_gcs_to_bigquery',
    default_args=default_args,
    description='Fetch data from an API, store in GCS, and load into BigQuery',
    schedule_interval='@daily',
)

# Configuration
bucket_name = 'your-gcs-bucket-name'
gcs_file_path = 'data/api_data.json'
local_file_path = '/tmp/api_data.json'
project_id = 'your-gcp-project-id'
dataset_id = 'your_bigquery_dataset'
table_id = 'your_bigquery_table'
api_url = 'https://api.example.com/data'  # Replace with your API URL

def fetch_api_data():
    response = requests.get(api_url)
    response.raise_for_status()  # Check if the request was successful
    data = response.json()
    with open(local_file_path, 'w') as f:
        json.dump(data, f)

# Task to fetch data from the API and save to local file
fetch_api_data_task = PythonOperator(
    task_id='fetch_api_data',
    python_callable=fetch_api_data,
    dag=dag,
)

# Task to upload the local file to GCS
upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_to_gcs',
    src=local_file_path,
    dst=gcs_file_path,
    bucket=bucket_name,
    google_cloud_storage_conn_id='google_cloud_default',
    dag=dag,
)

# Task to load data from GCS to BigQuery
load_to_bigquery_task = GoogleCloudStorageToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=bucket_name,
    source_objects=[gcs_file_path],
    destination_project_dataset_table=f'{project_id}:{dataset_id}.{table_id}',
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,
    google_cloud_storage_conn_id='google_cloud_default',
    bigquery_conn_id='google_cloud_default',
    dag=dag,
)

# Define task dependencies
fetch_api_data_task >> upload_to_gcs_task >> load_to_bigquery_task
