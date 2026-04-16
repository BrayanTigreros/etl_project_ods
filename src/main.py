import pandas as pd
from tabulate import tabulate
from log import log_progress
from extract import extract_incautaciones, profiling_csv
from extract_api import extract_gbif, profiling_api
from transform import transform_data
from load import save_dimensions_to_csv, load_to_dw

gbif_raw_path = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto\etl_project_ods-main\etl_project_ods-main\raw\gbif_raw.csv'

log_file = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto\etl_project_ods-main\etl_project_ods-main\logs\log_file.txt'

target_file = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto\etl_project_ods-main\petl_project_ods-main\transformed'

data_path = r'C:\Users\btigr\Documents\UAO\5\ETL\ETL_2026_1\proyecto\etl_project_ods-main\etl_project_ods-main\raw\incautaciones.csv'


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

    # Transform
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

    # Validity Check
    log_progress('Data validity check started', log_file)

    log_progress('Data validity check complete', log_file)


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


if __name__ == "__main__":
    main()