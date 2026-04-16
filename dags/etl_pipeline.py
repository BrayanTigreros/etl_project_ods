from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd
import os

# Paths
RAW_DATA_PATH = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto\etl_project_ods-main\etl_project_ods-main\data'
TEMP_CSV = "/opt/airflow/data/spotify_processing.csv"
TEMP_API = "/opt/airflow/data/gbif_processing.csv"

# Define the DAG using the @dag decorator
@dag(
    dag_id="proyecto_etl_2",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "proyecto", "datawarehouse", "incautaciones", "ods"],
    max_active_runs=1
)

def etl_pipeline():

# Extract task
    @task()
    def extract_csv():
        df_incautaciones = pd.read_csv(RAW_DATA_PATH)
        return TEMP_CSV
    
    @task()
    def extract_api():
        df_gbif = extract_api(RAW_DATA_PATH)
        return TEMP_API



# Execution order
    df_incautaciones = extract_csv()
    df_gbif = extract_api()

etl_pipeline()