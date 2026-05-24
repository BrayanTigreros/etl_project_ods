import pandas as pd
from tabulate import tabulate
from log import log_progress
from extract import extract_incautaciones, profiling_csv
from extract_api import extract_gbif, profiling_api
from transform import transform_data
from load import save_dimensions_to_csv, load_to_dw

from output_validation import output_data_validation
from input_validation import input_data_validation

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

import threading
import time

from kafka_src.producer_metrics import run_producer
from kafka_src.consumer_metrics import run_consumer

gbif_raw_path = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\data\raw\gbif_raw.csv'
log_file = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\logs\log_file.txt'
target_file = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\transformed'
data_path = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\data\raw\incautaciones.csv'

inp_validate = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\expectations\input'
out_validate = r'C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\expectations\output'

def main():
    # ETL process
    log_progress('Starting ETL process', log_file)

    # Extract
    log_progress('Extract phase started', log_file)
    df_incautaciones = extract_incautaciones(data_path)
    df_gbif = extract_gbif(data_path, gbif_raw_path)
    df_incautaciones = extract_incautaciones(data_path)
    print(tabulate(df_incautaciones.head(), headers='keys', tablefmt='psql'))
    log_progress("Extract phase complete", log_file)

    # Profiling
    log_progress('Data profiling started', log_file)
    profiling_csv(df_incautaciones)
    profiling_api(df_gbif)
    log_progress('Data profiling complete', log_file)

    # Validate input 
    log_progress('Input data validation started', log_file)
    input_data_validation(df_incautaciones, df_gbif, inp_validate)
    log_progress('Input data validation complete', log_file)

    # Transform and Clean
    log_progress('Transform phase started', log_file)
    df_transform = transform_data(df_incautaciones, df_gbif)

    print("\nDIM_TIEMPO")
    print(tabulate(df_transform["dim_tiempo"].head(), headers='keys', tablefmt='psql'))

    print("\nDIM_UBICACION")
    print(tabulate(df_transform["dim_ubicacion"].head(), headers='keys', tablefmt='psql'))

    print("\nDIM_ESPECIE")
    print(tabulate(df_transform["dim_especie"].head(), headers='keys', tablefmt='psql'))

    print("\nDIM_AUTORIDAD")
    print(tabulate(df_transform["dim_autoridad"].head(), headers='keys', tablefmt='psql'))

    print("\nFACT_INCAUTACIONES")
    print(tabulate(df_transform["fact_incautaciones"].head(), headers='keys', tablefmt='psql'))

    log_progress('Transform phase complete', log_file)

    # Validite output
    log_progress('Validate output tarted', log_file)
    output_data_validation(df_transform, out_validate)
    log_progress('Validate output complete', log_file)


    # Load
    log_progress('Load phase started', log_file)

    save_dimensions_to_csv(
        target_file,
        dim_tiempo=df_transform["dim_tiempo"],
        dim_ubicacion=df_transform["dim_ubicacion"],
        dim_especie=df_transform["dim_especie"],
        dim_autoridad=df_transform["dim_autoridad"],
        fact_incautaciones=df_transform["fact_incautaciones"]
    )

    load_to_dw(df_transform)
    log_progress('Load phase complete', log_file)

    # ETL process
    log_progress('ETL process finished successfully', log_file)

    # streaming kafka
    log_progress('Kafka streaming started', log_file)
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()

    time.sleep(2)          #este sleep es para asegurar que el consumer este listo antes de que el producer envie los mesajes

    run_producer(iterations=1)

    time.sleep(5)          #aqui espera que el consumer procese los mensajes
    log_progress('Kafka streaming complete', log_file)


if __name__ == "__main__":
    main()