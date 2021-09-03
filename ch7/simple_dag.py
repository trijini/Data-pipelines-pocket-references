from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# define DAG
dag = DAG(
    'simple_dag',
     description='A simple DAG',
     schedule_interval=timedelta(days=1),
     start_date = days_ago(1),
)

# define tasks
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 3',
    dag=dag,
)

t3 = BashOperator(
    task_id='print_end',
    depends_on_past=False,
    bash_command='echo \'end\'',
    dag=dag,
)

# define the dependencies between the tasks
t1 >> t2  # this means when the task t1 completes, t2 runs
t2 >> t3