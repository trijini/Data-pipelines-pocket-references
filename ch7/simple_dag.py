from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from p.simple_extract_upload import Copy, Extract


def execute(**kwargs):
    table = kwargs['table']
    file = kwargs['file']
    task = kwargs['task']

    if task == 'extract':
        s = Extract(table, file)
        return s.extract()
    elif task == 'copy':
        s = Copy(table, file)
        return s.upload()


with DAG(
    'simple_dag',
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
) as dag:

    # define tasks
    t1 = PythonOperator(
        task_id='extract',
        python_callable=execute,
        op_kwargs={
            'table': 'Orders',
            'file': 'order_extract.csv',
            'task': 'extract'
        },
        dag=dag,
    )

    t2 = PythonOperator(
        task_id='copy',
        python_callable=execute,
        op_kwargs={
            'table': 'Mock',
            'file': 'order_extract.csv',
            'task': 'copy'
        },
        dag=dag,
    )

t1 >> t2