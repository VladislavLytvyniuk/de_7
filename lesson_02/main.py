#script on google cloud function to push clouud storage

import requests
from google.cloud import storage
import requests

url = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales?date=2022-08-09' #&page=2
headers={'Authorization': '2b8d97ce57d401abd89f45b0079d8790edd940e6'}

bucket_name = 'test_de_course'

storage_client = storage.Client()

def upload_blob(bucket, data, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    data = str(data)
    blob.upload_from_string(data, content_type='application/json')


def get_request_to_api(url, headers, params):
    data = requests.get(url, headers=headers, params=params)
    return data.json()

def main (request):
    files_created = []

    try:
        request_json = request.get_json()
        #request_json = request
        date  = request_json.get('date') 
        raw_dir = request_json.get('raw_dir')
        del request_json

        if date is None:
            raise ValueError('You have to set the "date" parameter')

        if raw_dir is None:
            raise ValueError('You have to set the "raw_dir" parameter')

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)    

        params_send = {'date' : date,  'page' : 1 }
        
        itterations = 1
        
        while True:            
            data = get_request_to_api(url, headers, params_send)            
            if 'message' in data:
                if itterations == 1:
                    raise ValueError('Someting wrong with api')
                break
                
            print(data)
            print('\n')
            full_path = raw_dir + '/' + date + '_' + str(params_send['page']) + '.json'

            upload_blob(bucket, data, full_path)

            files_created.append(full_path)
            print(files_created)
                                        
            params_send['page'] += 1
            itterations += 1


        return {'success' : 'Files were written: ' + ', '.join(files_created)}

    except Exception as e:
        print(files_created)
        text_error = str(e) 

        if files_created:
            text_error += ' . Files were writtet: ' + ', '.join(files_created)

        print(text_error)
        return {'error' : text_error}