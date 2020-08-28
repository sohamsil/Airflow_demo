from airflow.models import DAG
from airflow.operators import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime,timedelta

DAG_ID = 'airflow-test'
DAG_OWNER_NAME = "Airflow"
ALERT_EMAIL_ADDRESSES = []
START_DATE = datetime(2020, 8, 28)
SCHEDULE_INTERVAL = None

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': True,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE
)

start = DummyOperator(
    task_id='start',
    dag=dag)

extract_script_path = "/scripts/extract.py"
transform_script_path = "/scripts/transform.py"
bq_script_path = "/scripts/load_to_bq.py"

extract_task = BashOperator(
            task_id="extract_task_worker",
            name="extract_task",
            bash_command='python {}'.format(extract_script_path),
            dag=dag)

transform_task = BashOperator(
            task_id="transform_task_worker",
            name="transform_task",
            bash_command='python {}'.format(transform_script_path),
            dag=dag)

bq_task = BashOperator(
            task_id="bq_task_worker",
            name="bq_task",
            bash_command='python {}'.format(bq_script_path),
            dag=dag)

start >> extract_task >> transform_task >> bq_task