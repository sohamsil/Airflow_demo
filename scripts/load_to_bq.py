from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import json

gcp_creds = json.load(open('<path-to-service-account-key>'))

# Extract json line file from GCP Transform Bucket
storage_client = storage.Client.from_service_account_json(gcp_creds)
tfbucket = storage_client.get_bucket('airflow-tf')
tfblob = tfbucket.get_blob('sample.json')

content = tfblob.download_as_string().decode("utf-8")

# Load transform json data to GCP BigQuery
bq_client = bigquery.Client.from_service_account_json(gcp_creds)

dataset_ref = bq_client.dataset('test')
table_ref = dataset_ref.table('demotest')
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = "WRITE_APPEND"
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True

job = bigquery.job.LoadJob(
    job_id='demotest'+'_'+datetime.now().strftime("%Y-%m-%d-%H-%M-%S"),
    source_uris=["gs://airflow-tf/sample.json"],
    client=bq_client,
    destination=table_ref,
    job_config=job_config)

print('Job result: ' + str(job.result()))

