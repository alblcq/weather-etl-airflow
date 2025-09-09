from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys, os

scripts_path = os.path.join(os.path.dirname(__file__), '..', 'scripts')
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

# Shared state for ETL run (as Airflow XComs or via files for real workflows)
etl_context = {}

def setup_logging_task():
    from scripts.utils import setup_logging
    setup_logging()

def get_config_task():
    from scripts.utils import get_config
    etl_context['cfg'] = get_config()

def generate_run_id_task():
    from scripts.utils import generate_etl_run_id
    etl_context['run_id'] = generate_etl_run_id()

def extract_weather_task():
    from scripts.extract_weather import extract_weather_data
    cfg = etl_context['cfg']
    api_key = cfg['OPENWEATHERMAP_API_KEY']
    cities = cfg['CITIES']
    etl_context['city_json_pairs'] = extract_weather_data(cities, api_key)

def load_raw_task():
    from scripts.load_snowflake import load_raw_to_snowflake
    load_raw_to_snowflake(etl_context['city_json_pairs'], etl_context['run_id'])

def transform_weather_task():
    from scripts.transform_weather import transform_weather
    etl_context['df'] = transform_weather(etl_context['city_json_pairs'], etl_context['run_id'])

def load_transformed_task():
    from scripts.load_snowflake import load_transformed_to_snowflake
    load_transformed_to_snowflake(etl_context['df'])

def validate_weather_task():
    from scripts.validate_weather_data import validate_weather_data
    validate_weather_data(etl_context['run_id'])

default_args = {
    'owner': 'alblcq',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    description='Weather ETL',
    schedule='0 7 * * *',
    start_date=datetime(2025, 9, 1),
    catchup=False
) as dag:
    
    t1 = PythonOperator(task_id='setup_logging', python_callable=setup_logging_task)
    t2 = PythonOperator(task_id='get_config', python_callable=get_config_task)
    t3 = PythonOperator(task_id='generate_etl_run_id', python_callable=generate_run_id_task)
    t4 = PythonOperator(task_id='extract_weather', python_callable=extract_weather_task)
    t5 = PythonOperator(task_id='load_raw', python_callable=load_raw_task)
    t6 = PythonOperator(task_id='transform_weather', python_callable=transform_weather_task)
    t7 = PythonOperator(task_id='load_transformed', python_callable=load_transformed_task)
    t8 = PythonOperator(task_id='validate_weather', python_callable=validate_weather_task)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8