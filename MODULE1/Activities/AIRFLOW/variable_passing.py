from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Luis Gustavo de Souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 29, 23),
    'email': ['luisouza98@gmail.com', 'igti@igti.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "dag_xcom",
    description="How to use XCOM in Apache Airflow",
    default_args=default_args,
    schedule_interval='*/2 * * * *'
)

get_data = BashOperator(
    task_id="get_data",
    dag=dag,
    bash_command="curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o ~/train.csv"
)


def calculate_mean_age():
    titanic = pd.read_csv('~/train.csv')
    return titanic.Age.mean()


def print_mean_age(**context):
    value = context['task_instance'].xcom_pull(task_ids='calculate_mean_age')
    print('Mean age in Titanic was {} years old'.format(value))


pyop_calculate_age = PythonOperator(
    task_id="calculate_mean_age",
    dag=dag,
    python_callable=calculate_mean_age
)

pyop_print_age = PythonOperator(
    task_id="print_age",
    dag=dag,
    python_callable=print_mean_age,
    provide_context=True
)


get_data >> pyop_calculate_age >> pyop_print_age
