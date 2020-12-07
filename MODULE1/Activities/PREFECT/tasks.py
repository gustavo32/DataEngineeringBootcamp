from datetime import datetime, timedelta
import prefect
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import pandas as pd

retry_delay = timedelta(minutes=1)
schedule = IntervalSchedule(interval=timedelta(minutes=2))

@task
def get_data():
    df = pd.read_csv('https://raw.githubusercontent.com/A3Data/hermione/master/hermione/file_text/train.csv')
    return df


@task
def titanic_mean_age(df):
    return df.Age.mean()

@task
def show_calculcated_mean(m):
    logger = prefect.context.get('logger')
    logger.info('The average age in Titanic was {}'.format(m))

@task
def show_dataframe(df):
    logger = prefect.context.get('logger')
    logger.info(df.head(3).to_json())


with Flow('Titanic01', schedule=schedule) as flow:
    df = get_data()
    m = titanic_mean_age(df)
    sm = show_calculcated_mean(m)
    sd = show_dataframe(df)

flow.register(project_name='IGTI', idempotency_key=flow.serialized_hash())
# flow.run_agent(token='D_0wWDFgx0e67I2IIbf7Ew')