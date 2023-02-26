from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import requests
import pandas as pd
from google.cloud import storage

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

FAKE_API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
FAKE_API_KEY = BaseHook.get_connection("FAKE_API_KEY").password
FAKE_API_HEADERS = {'Authorization': FAKE_API_KEY}
DATA_RANGE = ['2022-08-10', '2022-08-11']

def get_request_to_api_func():
    data = {}
    for DATE_NOW in DATA_RANGE:
        data[DATE_NOW] = []
        PARAMS = {'date' : DATE_NOW,  'page' : 1 }
        while True:            
            temp_data = requests.get(FAKE_API_URL, headers=FAKE_API_HEADERS, params=PARAMS).json() 
            if type(temp_data) == dict:
                break
            data[DATE_NOW] += temp_data
            PARAMS['page'] += 1
    return data
    

def push_to_gcs_func(ti):

    data = ti.xcom_pull(task_ids=['get_data_from_api'])[0]
    print(data)
    client = storage.Client()
    bucket = client.bucket('test_de_course')

    for k in data:
        k_split = k.split('-')
        year, month, day = k_split[0], k_split[1], k_split[2]
        gcs_path = f'lesson10/sales/v1/year={year}/month={month}/day={day}/data.csv'
        blob = bucket.blob(gcs_path)

        csv_data = pd.DataFrame(data[k]).to_csv(index=False)
        blob.upload_from_string(csv_data)

    return data   

with DAG(
    dag_id='process_sales_to_gcs',
    start_date=datetime(2023, 2, 8),
    schedule_interval="* 3 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    get_request_to_api_task = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_request_to_api_func,
        provide_context=True,
    )

    push_to_gcs_task = PythonOperator(
        task_id='push_to_gcs',
        python_callable=push_to_gcs_func,
        provide_context=True,
    )

    get_request_to_api_task >> push_to_gcs_task
