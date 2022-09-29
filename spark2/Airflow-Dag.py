from dateutil import parser
from datetime import datetime, timedelta
from datetime import timezone
from airflow import DAG
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from airflow.operators.dummy_operator import DummyOperator

prefix = 'cnelson2'

default_args = {
    'owner': 'Airflow',
    'retry_delay': timedelta(seconds=10),
    'depends_on_past': False,
    'start_date': datetime.today() - timedelta(days=2),
    'end_date':   datetime.today() - timedelta(days=-7)
}

dag = DAG(
    f'{prefix}-airflow-pipeline',
    default_args=default_args,
    schedule_interval='@yearly',
    catchup=False,
    is_paused_upon_creation=False
)

create_step1 = CDEJobRunOperator(
    task_id='create-dwh',
    dag=dag,
    job_name=f'pre-setupDW'
)

enrich_step2 = CDEJobRunOperator(
    task_id='enrich-dwh',
    dag=dag,
    job_name=f'enrich-ETL'
)

create_step1 >> enrich_step2
