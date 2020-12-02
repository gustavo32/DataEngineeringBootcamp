from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from zipfile import ZipFile

fn_data_folder = '/usr/local/airflow/data/'
fn_zipped_enade = fn_data_folder + 'enade.zip'
fn_enade_folder = fn_data_folder + 'microdados_enade_2019/2019/3.DADOS/'
fn_enade_file = fn_enade_folder + 'microdados_enade_2019.txt'
fn_filtered_data = fn_enade_folder + 'filtered_enade_data.csv'

default_args = {
    'owner': 'Luis Gustavo de Souza',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 30, 18),
    'email': ['luisouza98@gmail.com', 'igti@igti.com.br'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "dag_parallelism",
    description="How to use XCOM in Apache Airflow",
    default_args=default_args,
    schedule_interval='*/2 * * * *'
)

with dag:

    def unzip_file():
        with ZipFile(fn_zipped_enade, 'r') as zipped:
            zipped.extractall(fn_data_folder)

    def filter_data():
        interested_cols = ['CO_GRUPO', 'NU_IDADE', 'TP_SEXO', 'NT_GER',
                           'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
        df = pd.read_csv(fn_enade_file, sep=';', decimal=',',
                         usecols=interested_cols)
        df[(df.NU_IDADE > 20) &
           (df.NU_IDADE < 40) &
           (df.NT_GER > 0)].to_csv(fn_filtered_data)

    def get_mean_normalized_age():
        df = pd.read_csv(fn_filtered_data)
        df['mean_normalized_age'] = df.NU_IDADE - df.NU_IDADE.mean()
        df[['mean_normalized_age']].to_csv(
            fn_enade_folder+'mean_normalized_age.csv')

    def get_squared_mean_normalized_age():
        df = pd.read_csv(fn_enade_folder + 'mean_normalized_age.csv')
        df['squared_mean_normalized_age'] = df['mean_normalized_age'] ** 2
        df[['squared_mean_normalized_age']].to_csv(
            fn_enade_folder + 'squared_mean_normalized_age.csv'
        )

    def get_marital_status():
        df = pd.read_csv(fn_filtered_data)
        df['marital_status'] = df.QE_I01.replace({
            'A': 'SINGLE',
            'B': 'MARRIED',
            'C': 'DIVORCED',
            'D': 'WIDOWED',
            'E': 'OTHERS'
        })

        df[['marital_status']].to_csv(
            fn_enade_folder + 'marital_status.csv'
        )

    def get_skin_color():
        df = pd.read_csv(fn_filtered_data)
        df['skin_color'] = df.QE_I01.replace({
            'A': 'WHITE',
            'B': 'BLACK',
            'C': 'YELLOW',
            'D': 'BROWN',
            'E': 'INDIGENOUS',
            'F': '',
            ' ': ''
        })
        df[['skin_color']].to_csv(
            fn_enade_folder + 'skin_color.csv'
        )

    def join_data():
        df = pd.read_csv(fn_filtered_data)
        df_mean_normalized_age = pd.read_csv(
            fn_enade_folder+'mean_normalized_age.csv')
        df_squared_mean_normalized_age = pd.read_csv(
            fn_enade_folder + 'squared_mean_normalized_age.csv')
        df_marital_status = pd.read_csv(fn_enade_folder + 'marital_status.csv')
        df_skin_color = pd.read_csv(fn_enade_folder + 'skin_color.csv')

        pd.concat([
            df,
            df_mean_normalized_age,
            df_squared_mean_normalized_age,
            df_marital_status,
            df_skin_color
        ], axis=1).to_csv(fn_enade_folder + 'transformed_data.csv')

    baop_start_process = BashOperator(
        task_id='start_process',
        bash_command='echo "Preprocessing of ENADE Data!"'
    )

    baop_get_data = BashOperator(
        task_id='get_data',
        bash_command='curl http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip -o ' + fn_zipped_enade
    )

    pyop_unzip_file = PythonOperator(
        task_id='unzip_file',
        python_callable=unzip_file
    )

    pyop_filter_data = PythonOperator(
        task_id='filter_data',
        python_callable=filter_data
    )

    pyop_mean_normalize_age = PythonOperator(
        task_id='mean_normalize_age',
        python_callable=get_mean_normalized_age
    )

    pyop_squared_mean_normalize_age = PythonOperator(
        task_id='squared_mean_normalize_age',
        python_callable=get_squared_mean_normalized_age
    )

    pyop_marital_status = PythonOperator(
        task_id='marital_status',
        python_callable=get_marital_status
    )

    pyop_skin_color = PythonOperator(
        task_id='skin_color',
        python_callable=get_skin_color
    )

    pyop_join_data = PythonOperator(
        task_id='join_data',
        python_callable=join_data
    )

    baop_start_process >> baop_get_data >> pyop_unzip_file >> pyop_filter_data
    pyop_filter_data >> pyop_mean_normalize_age >> pyop_squared_mean_normalize_age
    pyop_filter_data >> [pyop_marital_status, pyop_skin_color]

    pyop_join_data.set_upstream([
        pyop_squared_mean_normalize_age,
        pyop_marital_status,
        pyop_skin_color
    ])
