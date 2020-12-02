from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random

default_args = {
    'owner': 'Luis Gustavo de Souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 30, 1),
    'email': ['luisouza98@gmail.com', 'igti@igti.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "dag_conditional",
    description="How to use conditional in Apache Airflow",
    default_args=default_args,
    schedule_interval='*/2 * * * *'
)

get_data = BashOperator(
    task_id="get_data",
    dag=dag,
    bash_command="curl https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv -o /usr/local/airflow/data/train.csv"
)


def choose_man_female():
    return random.choice(['male', 'female'])


def branch_sex(**context):
    value = context['task_instance'].xcom_pull(task_ids='choose_male_female')
    if value == 'male':
        return 'branch_male'
    elif value == 'female':
        return 'branch_female'


def mean_age_male():
    titanic = pd.read_csv('/usr/local/airflow/data/train.csv')
    print('Males present in Titanic was, on average, {} years old'.format(
        titanic[titanic.Sex == 'male'].Age.mean()))


def mean_age_female():
    titanic = pd.read_csv('/usr/local/airflow/data/train.csv')
    print('Females present in Titanic were, on average, {} years old'.format(
        titanic[titanic.Sex == 'female'].Age.mean()))


pyop_chooser = PythonOperator(
    task_id="choose_male_female",
    dag=dag,
    python_callable=choose_man_female
)

branch_pyop_sex = BranchPythonOperator(
    task_id='branch_sex',
    python_callable=branch_sex,
    dag=dag,
    provide_context=True
)

pyop_mean_male = PythonOperator(
    task_id='branch_male',
    python_callable=mean_age_male,
    dag=dag
)

pyop_mean_female = PythonOperator(
    task_id='branch_female',
    python_callable=mean_age_female,
    dag=dag
)

get_data >> pyop_chooser >> branch_pyop_sex >> [
    pyop_mean_male, pyop_mean_female]
