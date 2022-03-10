from operator import ge
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from src.stream_flight_data.kafka_producer import generate_stream

PATH_STREAM_TRANSACTIONS = "/data/transactions.json"
PATH_STREAM_LOCATIONS = "/data/locations.json"

Topic1 = 'transactions'
Topic2 = 'locations'


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),      # this in combination with catchup=False ensures the DAG being triggered from the current date onwards along the set interval
    'provide_context': True,                            # this is set to True as we want to pass variables on from one task to another
}

dag = DAG(
    dag_id='data_stream_DAG',
    default_args=args,
    schedule_interval= '@once',             # set interval
    catchup=False,                          # indicate whether or not Airflow should do any runs for intervals between the start_date and the current date that haven't been run thus far
)

generate_transactions_stream = PythonOperator(
    task_id='generate_transactions_stream',
    python_callable=generate_stream,
    op_kwargs={'path_stream': PATH_STREAM_TRANSACTIONS,        # input arguments
               'Topic': Topic1},
    dag=dag,
)

generate_locations_stream = PythonOperator(
    task_id='generate_locations_stream',
    python_callable=generate_stream,
    op_kwargs={'path_stream': PATH_STREAM_LOCATIONS,
               'Topic': Topic2},
    dag=dag,
)

generate_transactions_stream
generate_locations_stream
