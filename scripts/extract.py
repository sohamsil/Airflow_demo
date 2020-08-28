import pandas as pd
import json
from google.cloud import storage

# Load data from local
df = pd.read_csv('C:/airflow/dags/data/demotest.csv')

# Write json lines to local
df.to_json('C:/airflow/dags/data/demotest_raw.json', orient='records', lines=True)

# Load json line local file to GCP Raw Bucket
gcp_creds = json.load(open('<path-to-service-account-key>'))

storage_client = storage.Client.from_service_account_json(gcp_creds)
rawbucket = storage_client.get_bucket('airflow-raw')
rawblob = rawbucket.blob('sample.json')
rawblob.upload_from_filename('C:/airflow/dags/data/demotest_raw.json')