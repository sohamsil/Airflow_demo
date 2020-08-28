import pandas as pd
import json
from google.cloud import storage

# Extract json line file from GCP Raw Bucket
gcp_creds = json.load(open('<path-to-service-account-key>'))

storage_client = storage.Client.from_service_account_json(gcp_creds)
rawbucket = storage_client.get_bucket('airflow-raw')
rawblob = rawbucket.get_blob('sample.json')

content = rawblob.download_as_string().decode("utf-8")

# Store raw data in Pandas and transform
df = pd.read_json(content,lines=True)
df = df.drop_duplicates()
df = df.fillna('')

# Export transformed data to json line in local
df.to_json('C:/airflow/dags/data/demotest_tf.json', orient='records', lines=True)

# Load json line local file to GCP Transform Bucket
gcp_creds = json.load(open('<path-to-service-account-key>'))

storage_client = storage.Client.from_service_account_json(gcp_creds)
tfbucket = storage_client.get_bucket('airflow-tf')
tfblob = tfbucket.blob('sample.json')
tfblob.upload_from_filename('C:/airflow/dags/data/demotest_tf.json')

