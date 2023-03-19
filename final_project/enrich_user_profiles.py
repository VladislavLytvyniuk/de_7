from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import bigquery

def enrich_user_profiles():
    sql_query = f"""
    CREATE OR REPLACE TABLE de_7_gold.enrich_user_profiles AS 
    SELECT 
        client_id,
        CASE 
            WHEN p.full_name is not null then LOWER(SPLIT(p.full_name, ' ')[OFFSET(0)])
            ELSE LOWER(c.first_name)
        END as first_name,
        CASE 
            WHEN p.full_name is not null then LOWER(SPLIT(p.full_name, ' ')[OFFSET(1)])
            ELSE LOWER(c.last_name)
        END as last_name,
        email,
        c.registration_date registration_date,
        p.birth_date,
        p.phone_number,
        LOWER(IFNULL(p.state, c.state)) as state
    FROM `de_7_silver.customers` c
    LEFT JOIN  `de_7_silver.user_profiles` as p USING(email)
    QUALIFY row_number() over (partition by client_id) = 1
    """

    bq_client = bigquery.Client()
    job = bq_client.query(
        sql_query,
        job_id_prefix="de_7_enrich_user_profiles_"
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
    dag_id='enrich_user_profiles',
    start_date=datetime(2100, 2, 8),
    schedule_interval="* 3 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    enrich_user_profiles_task = PythonOperator(
        task_id='enrich_user_profiles_1',
        python_callable=enrich_user_profiles,
        provide_context=True,
    )

    enrich_user_profiles_task


