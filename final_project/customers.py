from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime

def customers():
    # EXAMPLE DATE
    #yesterday = datetime.strptime('2022-08-05', '%Y-%m-%d')
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    FORMATTED_YESTERDAY = yesterday.strftime('%Y-%m-%-d')

    GCS_BACKET = 'test_de_course_2'
    PROJECT_ID = 'project'
    DATASET_BRONZE_ID = 'de_7_bronze'
    DATASET_SILVER_ID = 'de_7_silver'
    TABLE_NAME = 'customers'

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    SCHEMA_TABLE = [
        bigquery.SchemaField('Id', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('FirstName', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('LastName', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Email', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('RegistrationDate', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('State', 'STRING', mode='NULLABLE')
    ]    

    GCS_JOB_CONFIG = bigquery.LoadJobConfig(
        schema=SCHEMA_TABLE,
        skip_leading_rows=1,
        field_delimiter=',',
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV
    )


    BRONZE_TABLE_ID = f'{PROJECT_ID}.{DATASET_BRONZE_ID}.{TABLE_NAME}'
    SILVER_TABLE_ID = f'{PROJECT_ID}.{DATASET_SILVER_ID}.{TABLE_NAME}'

    TABLE_REF = bq_client.dataset(DATASET_BRONZE_ID).table(TABLE_NAME)
    TABLE_OBJECT = bigquery.Table(TABLE_REF, schema=SCHEMA_TABLE)

    bq_client.delete_table(TABLE_REF, not_found_ok=True)
    bq_client.create_table(TABLE_REF)

    GCS_PREFIX_PATH = f'customers/{FORMATTED_YESTERDAY}/' 

    files = storage_client.list_blobs(GCS_BACKET, prefix=GCS_PREFIX_PATH)
    for blob in files:
        gcs_file_url = 'gs://' + GCS_BACKET + '/' + blob.name    
        print(gcs_file_url)
        load_job = bq_client.load_table_from_uri(
            gcs_file_url,
            BRONZE_TABLE_ID,
            location="US",  
            job_config=GCS_JOB_CONFIG,
            job_id_prefix="de_7_process_customers_"
        )  # Make an API request.

        job_result = load_job.result()  # Waits for the job to complete.
        print(f"Result: {job_result}")


    sql_query = f"""
    CREATE TABLE IF NOT EXISTS `{SILVER_TABLE_ID}`(
        update_date date,
        client_id int,
        first_name string,
        last_name string,
        email string,
        registration_date date,
        state string
    )
    PARTITION BY update_date;

    # тут напевне краще було би merge into
    INSERT INTO `{SILVER_TABLE_ID}` (update_date, client_id, first_name, last_name, email, registration_date, state)
    SELECT
        DATE('{FORMATTED_YESTERDAY}') as update_date,
        CAST(Id as int) as client_id,
        FirstName as first_name,
        LastName as last_name,
        Email as email,
        DATE(RegistrationDate) as registration_date,  
        State as state                       
    FROM `{BRONZE_TABLE_ID}`
    LEFT JOIN (
        SELECT CAST(client_id as string) as Id
        FROM `{SILVER_TABLE_ID}` 
    ) as old USING(Id)
    WHERE old.Id is null
    ;
    """

    bq_client = bigquery.Client()
    job = bq_client.query(
        sql_query,
        job_id_prefix="de_7_process_customers_"
    )
    print(job.result()) # Waits for the job to complete.     


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

with DAG(
    dag_id='customers',
    start_date=datetime(2023, 2, 8),
    schedule_interval="* 3 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    customers_task = PythonOperator(
        task_id='customers_1',
        python_callable=customers,
        provide_context=True,
    )

    customers_task  














