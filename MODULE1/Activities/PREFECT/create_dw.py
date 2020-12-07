from datetime import datetime, timedelta
import pendulum
import prefect
from prefect import task, Flow
from prefect.schedules import CronSchedule
import pandas as pd
from io import BytesIO
import zipfile
import requests
import sqlalchemy
import psycopg2

schedule = CronSchedule(
    cron= '*/10 * * * *',
    start_date=pendulum.datetime(2020, 12, 5, 14, tz="America/Sao_Paulo")
)


@task
def get_raw_date():
    url = 'http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip'
    filebytes = BytesIO(requests.get(url).content)

    zipped = zipfile.ZipFile(filebytes)
    zipped.extractall()
    return './microdados_enade_2019/2019/3.DADOS/'


@task
def apply_filters(path):
    interested_cols = ['CO_GRUPO', 'NU_IDADE', 'TP_SEXO', 'NT_GER',
                           'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 'QE_I04', 'QE_I05', 'QE_I08']
    df = pd.read_csv(path + 'microdados_enade_2019.txt', sep=';', decimal=',',
                        usecols=interested_cols)
    df[(df.NU_IDADE > 20) &
        (df.NU_IDADE < 40) &
        (df.NT_GER > 0)]

    return df


@task
def get_mean_normalized_age(df):
    coppied_df = df.copy()
    coppied_df['mean_normalized_age'] = coppied_df.NU_IDADE - coppied_df.NU_IDADE.mean()
    return coppied_df[['mean_normalized_age']]


@task
def get_squared_mean_normalized_age(df):
    coppied_df = df.copy()
    coppied_df['squared_mean_normalized_age'] = coppied_df['mean_normalized_age'] ** 2
    return coppied_df[['squared_mean_normalized_age']]


@task
def get_marital_status(df):
    coppied_df = df.copy()
    coppied_df['marital_status'] = coppied_df.QE_I01.replace({
        'A': 'SINGLE',
        'B': 'MARRIED',
        'C': 'DIVORCED',
        'D': 'WIDOWED',
        'E': 'OTHERS'
    })

    return coppied_df[['marital_status']]


@task
def get_skin_color(df):
    coppied_df = df.copy()
    coppied_df['skin_color'] = coppied_df.QE_I01.replace({
        'A': 'WHITE',
        'B': 'BLACK',
        'C': 'YELLOW',
        'D': 'BROWN',
        'E': 'INDIGENOUS',
        'F': '',
        ' ': ''
    })
    return coppied_df[['skin_color']]


@task
def join_data(dfs):
    final = pd.concat(dfs, axis=1)

    logger = prefect.context.get('logger')
    logger.info(final.head(2).to_json())

    return final


@task
def write_dw(df):
    engine = sqlalchemy.create_engine(
            'postgresql://postgres:123456@localhost:5432/enade')
    df.to_sql('enade', con=engine, index=False, if_exists='replace', method='multi', chunksize=100000)


with Flow('Enade', schedule) as flow:
    path = get_raw_date()
    df = apply_filters(path)
    normalized_mean_age = get_mean_normalized_age(df)
    normalized_squared_mean_age = get_squared_mean_normalized_age(normalized_mean_age)
    marital_status = get_marital_status(df)
    skin_color = get_skin_color(df)
    final = join_data([
        df,
        normalized_mean_age,
        normalized_squared_mean_age,
        marital_status,
        skin_color
    ])
    dw = write_dw(final)

flow.register(project_name='IGTI', idempotency_key=flow.serialized_hash())
flow.run_agent(token='D_0wWDFgx0e67I2IIbf7Ew')
