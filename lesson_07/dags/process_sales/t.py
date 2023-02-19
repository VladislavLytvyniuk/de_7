from datetime import datetime 
import requests
import os
import logging
import sys
import json



DATE_NOW = datetime.today().strftime('%Y-%m-%d')
FAKE_API_KEY = os.getenv('FAKE_API_KEY')
FAKE_API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales' #&page=2
FAKE_API_HEADERS = {'Authorization': FAKE_API_KEY}


DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': 30,
}

# dag = DAG(
#     dag_id='test_process_sales',
#     start_date=datetime(2023, 1, 9),
#     # end_date=datetime(2023, 1, 15),
#     schedule_interval="*/5 * * * *",
#     catchup=True,
#     default_args=DEFAULT_ARGS,
# )


def get_request_to_api(url, headers, params):
    data = requests.get(url, headers=headers, params=params)
    return data.json()


def extract_data_from_api ():

    DATE_NOW = '2022-08-09'
    PARAMS = {'date' : DATE_NOW,  'page' : 1 }

    itterations = 1

    data = get_request_to_api(FAKE_API_URL, FAKE_API_HEADERS, PARAMS)    

    
    return {'data' : data}


data = extract_data_from_api()
print(data)

with open("t1.json", "w") as f:
    json.dump(data, f)