from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime

def sales ():
    # EXAMPLE DATE
    #yesterday = datetime.strptime('2022-09-01', '%Y-%m-%d')
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    FORMATTED_YESTERDAY = yesterday.strftime('%Y-%m-%-d')

    GCS_BACKET = 'test_de_course_2'
    PROJECT_ID = 'project'
    DATASET_BRONZE_ID = 'de_7_bronze'
    DATASET_SILVER_ID = 'de_7_silver'
    TABLE_NAME = 'sales'

    bq_client = bigquery.Client()
    storage_client = storage.Client()

    SCHEMA_TABLE = [
        bigquery.SchemaField('CustomerId', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('PurchaseDate', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Product', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Price', 'STRING', mode='NULLABLE')
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

    bq_client.delete_table(TABLE_REF, not_found_ok=True)
    bq_client.create_table(TABLE_REF)

    GCS_PREFIX_PATH = f'sales/{FORMATTED_YESTERDAY}/' 
    files = storage_client.list_blobs(GCS_BACKET, prefix=GCS_PREFIX_PATH)
    for blob in files:
        gcs_file_url = 'gs://' + GCS_BACKET + '/' + blob.name    
        load_job = bq_client.load_table_from_uri(
            gcs_file_url,
            BRONZE_TABLE_ID,
            location="US",  
            job_config=GCS_JOB_CONFIG,
            job_id_prefix="de_7_process_sales_"
        )  # Make an API request.

        job_result = load_job.result()  # Waits for the job to complete.
        print(f"Result: {job_result}")


    sql_query = f"""
    CREATE TABLE IF NOT EXISTS `{SILVER_TABLE_ID}`   (
        client_id int,
        purchase_date date,
        product_name string,
        price float64
    )
    PARTITION BY purchase_date;

    DELETE FROM `{SILVER_TABLE_ID}` WHERE purchase_date = DATE('{FORMATTED_YESTERDAY}');
    INSERT INTO `{SILVER_TABLE_ID}` (client_id, purchase_date, product_name, price)
    SELECT 
        CAST(CustomerId as int) as client_id,
        CAST (REPLACE(REPLACE(PurchaseDate, '/', '-'),  '2022-Aug-30', '2022-08-30') as date) as purchase_date,
        Product as product_name,
        CAST(REPLACE(REPLACE(Price, '$', ' '), 'USD', '') as float64) as price
    FROM `{BRONZE_TABLE_ID}`
    #WHERE DATE(PurchaseDate) = DATE('{FORMATTED_YESTERDAY}');
    """

    bq_client = bigquery.Client()
    job = bq_client.query(
        sql_query,
        job_id_prefix="de_7_process_sales_"
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
    dag_id='sales_test',
    start_date=datetime(2023, 2, 8),
    schedule_interval="* 3 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    sales_task = PythonOperator(
        task_id='sales_1',
        python_callable=sales,
        provide_context=True,
    )

    sales_task    