from datetime import timedelta
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago


with DAG(
    'email_dag',
    description='A simple email alerting dag.',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
) as dag:

    email = EmailOperator(
        task_id='send_email',
        to='your_email@domain.com',
        subject='Airflow email alert',
        html_content='Email content here...',
        dag=dag,
    )
