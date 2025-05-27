import os 
import time
import json
import requests
import pickle
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

# Defin client parameters
client_id = os.getenv('GOOGLE_CLIENT_ID')
client_secret = os.getenv('GOOGLE_CLIENT_SECRET')
refresh_token = os.getenv('GOOGLE_REFRESH_TOKEN')

airflow_home = os.environ.get("AIRFLOW_HOME")
data_path = Path(airflow_home) / "dags" /"fitness_api_etl" / "data"

#create data directory if it doesn't exist
os.makedirs(data_path, exist_ok=True)



def get_access_token(client_id, client_secret, refresh_token):
    url = 'https://oauth2.googleapis.com/token'
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': refresh_token,
        'grant_type': 'refresh_token'
    }

    r = requests.post(url, data=data)
    r.raise_for_status()
    return r.json()['access_token']



def get_data_sources(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    url = 'https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate'

    now = int(time.time() * 1000)
    one_day = 86400000
    start = now - 7*one_day

    body = {
        "aggregateBy": [
        {
            "dataTypeName": "com.google.active_minutes",
            "dataSourceId": "derived:com.google.active_minutes:com.google.android.gms:merge_active_minutes",
        },
        {
            "dataTypeName": "com.google.distance.delta",
            "dataSourceId": "derived:com.google.distance.delta:com.google.android.gms:merge_distance_delta",
        },
        {
            "dataTypeName": "com.google.height",
            "dataSourceId": "derived:com.google.height:com.google.android.gms:merge_height",
        },
        {
            "dataTypeName": "com.google.sleep.segment",
        },
        {
            "dataTypeName": "com.google.body.temperature",

        },
        {
            "dataTypeName": "com.google.step_count.delta",
            "dataSourceId": "derived:com.google.step_count.delta:com.google.android.gms:estimated_steps",
        },
        {
            "dataTypeName": "com.google.calories.expended",
            "dataSourceId": "derived:com.google.calories.expended:com.google.android.gms:merge_calories_expended",
        },
        {
            "dataTypeName": "com.google.heart_rate.bpm",
        },
        {
            "dataTypeName": "com.google.weight",
            "dataSourceId": "derived:com.google.weight:com.google.android.gms:merge_weight",
        },
        {
            "dataTypeName": "com.google.blood_pressure",
        },
        {
            "dataTypeName": "com.google.blood_glucose",
        },   
        {
            "dataTypeName": "com.google.nutrition",

        },
        ],
        "bucketByTime": { "durationMillis": one_day },
        "startTimeMillis": start,
        "endTimeMillis": now
    }

    response = requests.post(url, headers=headers, json=body)

    if response.status_code != 200:
        print("Erreur:", response.text)
        return None

    return response.json()


MAPPING_DICT = {
    'com.google.active_minutes': 'active_minutes',
    'com.google.distance.delta': 'distance_delta',
    'com.google.height.summary': 'height',
    'com.google.sleep.segment': 'sleep_segment',
    'com.google.body.temperature.summary': 'body_temperature',
    'com.google.step_count.delta': 'step_count_delta',
    'com.google.calories.expended': 'calories_expended',
    'com.google.heart_rate.summary': 'heart_rate_bpm',
    'com.google.weight.summary': 'weight',
    'com.google.blood_pressure.summary': 'blood_pressure',
    'com.google.blood_glucose.summary': 'blood_glucose',
    'com.google.nutrition.summary': 'nutrition',
}



def parse_request_response(response):
    data_list = []

    for bucket in response['bucket']:
        data = {}
        start_time = bucket['startTimeMillis']

        # Convert start time to datetime object
        millis = int(start_time)
        dt = datetime.fromtimestamp(millis / 1000)
        data.update({'timestamp' : dt})

        for dataset in bucket.get('dataset', []):
            data_source = dataset.get('dataSourceId')
            points = dataset.get('point', [])

            # Try to get data type from the first point if it exists, otherwise fallback to dataset source ID
            data_type = points[0]['dataTypeName'] if points else data_source.split(":")[1]  # fallback from dataset ID

            # Map the data type
            mapped_key = MAPPING_DICT.get(data_type, data_type)

            if points:
                # Handle only the first value of the first point
                values = points[0].get('value', [])
                if values:
                    value_entry = values[0]
                    if 'intVal' in value_entry:
                        val = value_entry['intVal']
                    elif 'fpVal' in value_entry:
                        val = value_entry['fpVal']
                    else:
                        val = None
                else:
                    val = None
            else:
                val = None  # No data in this time window

            data.update({mapped_key: val})
        data_list.append(data)

    return data_list




def save_data_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")


def get_fitness_data():
    print("Starting the ETL process...")
    # Get credentials
    access_token = get_access_token(client_id, client_secret, refresh_token)
    print("Credentials obtained.")
    print("Requesting Data")
    response = get_data_sources(access_token)
    print("Data sucessfuly acquired")
    if response:
        data = parse_request_response(response)
        save_data_to_csv(data, data_path/'fitness_data.csv')
    else:
        print("No data received.")

if __name__ == "__main__":
    get_fitness_data()
    

