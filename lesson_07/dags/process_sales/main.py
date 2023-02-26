from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import requests
import fastavro


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

FAKE_API_KEY = BaseHook.get_connection("FAKE_API_KEY").password
FAKE_API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
FAKE_API_HEADERS = {'Authorization': FAKE_API_KEY}


def get_request_to_api(**context):
    data = []
    DATE_NOW = context['dag_run'].conf.get('date_now')
    PARAMS = {'date' : DATE_NOW,  'page' : 1 }
    while True:            
        temp_data = requests.get(FAKE_API_URL, headers=FAKE_API_HEADERS, params=PARAMS).json() 
        if type(temp_data) == dict:
            break
        
        data += temp_data
        PARAMS['page'] += 1
    return data

def format_data_to_avro(ti):
    schema_str = """
    {
        "type": "record",
        "name": "Purchase",
        "fields": [
            {"name": "client", "type": "string"},
            {"name": "purchase_date", "type": "string"},
            {"name": "product", "type": "string"},
            {"name": "price", "type": "int"}
        ]
    }
    """
    with open("purchase.avsc", "w") as schema_file:
        schema_file.write(schema_str)

    parsed_schema = fastavro.schema.load_schema("purchase.avsc")

    data = ti.xcom_pull(task_ids=['extract_data_from_api'])

    dict_data = [    
        {        
            "client": d["client"],
            "purchase_date": d["purchase_date"],
            "product": d["product"],
            "price": d["price"]
        }
        for d in data
    ]

    with open("my_data.avro", "wb") as out:
        fastavro.writer(out, parsed_schema, dict_data)


with DAG(
    dag_id='process_sales',
    start_date=datetime(2023, 2, 8),
    schedule_interval="* 3 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    get_data_from_api = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_request_to_api,
        provide_context=True,
    )

    format_data = PythonOperator(
        task_id='format_data_to_avro',
        python_callable=format_data_to_avro,
        provide_context=True,
    )

    get_data_from_api >> format_data
