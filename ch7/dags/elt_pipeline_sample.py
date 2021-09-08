from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from p.stage import Extract, Load


def execute(**kwargs):
    table = kwargs['table']
    file = kwargs['file']
    task = kwargs['task']
    database_config = ['database', 'username', 'password', 'host', 'port']
    s3_config = ['access_key', 'secret_key', 'bucket_name']

    if task == 'extract':
        s = Extract(table, file)
        config_name = 'postgres_config'
        s.extract(config_name, database_config)
        s.stage_to_s3('aws_boto_credentials', s3_config)
    elif task == 'load':
        s = Load(table, file)
        config_name = 'aws_creds'
        s.upload(config_name, database_config)
        

with DAG(
    'elt_pipeline_sample',
    description='A sample ELT pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
) as dag:

    extract_orders_task = PythonOperator(
        task_id='extract_order_data',
        python_callable=execute,
        op_kwargs={
            'table': 'Orders',
            'file': 'order_extract.csv',
            'task': 'extract',
        },
        dag=dag,
    )

    extract_customers_task = PythonOperator(
        task_id='extract_customer_data',
        python_callable=execute,
        op_kwargs={
            'table': 'Customers',
            'file': 'customer_extract.csv',
            'task': 'extract',
        },
        dag=dag,
    )

    load_orders_task = PythonOperator(
        task_id='load_order_data',
        python_callable=execute,
        op_kwargs={
            'table': 'Orders',
            'file': 'order_extract.csv',
            'task': 'load',
        },
        dag=dag,
    )

    load_customers_task = PythonOperator(
        task_id='load_customer_data',
        python_callable=execute,
        op_kwargs={
            'table': 'Customers',
            'file': 'customer_extract.csv',
            'task': 'load',
        },
        dag=dag,
    )

    revenue_model_task = PostgresOperator(
        task_id='build_data_model',
        postgres_conn_id='redshift_dw',
        sql='sql/order_revenue_model.sql',
        dag=dag,
    )

    extract_orders_task >> load_orders_task
    extract_customers_task >> load_customers_task
    load_orders_task >> revenue_model_task
    load_customers_task >> revenue_model_task