from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime

def user_profiles():
    GCS_BACKET = 'test_de_course_2'
    PROJECT_ID = 'project'
    DATASET_GOLD_ID = 'de_7_silver'
    TABLE_NAME = 'user_profiles'

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    SCHEMA_TABLE = [
        bigquery.SchemaField('email', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('full_name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('state', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('birth_date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('phone_number', 'STRING', mode='NULLABLE')
    ]    

    GCS_JOB_CONFIG = bigquery.LoadJobConfig(
        schema=SCHEMA_TABLE,
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )

    GOLD_TABLE_ID = f'{PROJECT_ID}.{DATASET_GOLD_ID}.{TABLE_NAME}'

    TABLE_REF = bq_client.dataset(DATASET_GOLD_ID).table(TABLE_NAME)

    bq_client.delete_table(TABLE_REF, not_found_ok=True)

    GCS_PREFIX_PATH = f'user_profiles/' 
    files = storage_client.list_blobs(GCS_BACKET, prefix=GCS_PREFIX_PATH)
    for blob in files:
        gcs_file_url = 'gs://' + GCS_BACKET + '/' + blob.name    
        load_job = bq_client.load_table_from_uri(
            gcs_file_url,
            GOLD_TABLE_ID,
            location="US",  
            job_config=GCS_JOB_CONFIG,
            job_id_prefix="de_7_process_sales_"
        )  # Make an API request.

        job_result = load_job.result()  # Waits for the job to complete.
        print(f"Result: {job_result}")

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

with DAG(
    dag_id='user_profiles_1',
    start_date=datetime(2100, 2, 8),
    schedule_interval="* 3 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    customers_task = PythonOperator(
        task_id='user_profiles_1',
        python_callable=user_profiles,
        provide_context=True,
    )

    customers_task      