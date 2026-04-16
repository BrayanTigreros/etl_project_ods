from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import os

# Paths
RAW_DATA_PATH = '/opt/airflow/data/incautaciones.csv'
RAW_DATA_API = '/opt/airflow/data/gbif_raw.csv'

# Define the DAG using the @dag decorator
@dag(
    dag_id="proyecto_etl_2",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule="@daily",
    tags=["etl", "proyecto", "datawarehouse", "incautaciones", "ods"],
    max_active_runs=1
)

def etl_pipeline():

# Extract task
    @task()
    def extract_csv():
        df_incautaciones = pd.read_csv(RAW_DATA_PATH)
        return df_incautaciones


    @task()
    def extract_api():
        df_api = pd.read_csv(RAW_DATA_API)
        return df_api
    

# Execution order
    df_incautacion_csv = extract_csv()
    df_api = extract_api()

etl_pipeline()