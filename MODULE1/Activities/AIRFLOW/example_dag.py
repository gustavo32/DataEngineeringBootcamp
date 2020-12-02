from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'Luis Gustavo de Souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 28),
    'email': ['luisouza98@gmail.com', 'igti@igti.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "example_dag",
    description="This is a hello world using bash operator in Apache Airflow",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
)

hello_bash = BashOperator(
    task_id="hello_bash",
    dag=dag,
    bash_command="echo 'This is a hello world using bash operator'"
)


def say_hello():
    print('Hello World!')


hello_python = PythonOperator(
    task_id="hello_python",
    dag=dag,
    python_callable=say_hello
)

hello_bash >> hello_python
