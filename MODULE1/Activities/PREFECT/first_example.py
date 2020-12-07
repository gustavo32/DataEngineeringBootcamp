import prefect
from prefect import task, Flow

@task
def say_hello():
    logger = prefect.context.get('logger')
    logger.info('This is the first example using Prefect!')


with Flow('first_example') as flow:
    say_hello()

flow.register('First Example')
flow.run_agent()