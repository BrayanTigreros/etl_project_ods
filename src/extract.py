import pandas as pd
from ydata_profiling import ProfileReport

def extract_incautaciones(path):
    df = pd.read_csv(path, sep=",", encoding="utf-8")

    df.columns = [
        "anio",
        "departamento",
        "municipio",
        "lugar_decomiso",
        "situacion",
        "autoridad_que_incauto",
        "tipo_especie",
        "nombre_comun",
        "nombre_cientifico",
        "cantidad"
    ]

    return df

def profiling_csv(df):
    profile = ProfileReport(df, title="Data Profiling Report CSV", explorative=True)

    # Or save the report to an HTML file
    profile.to_file(r"C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\profiling\csv_profiling_report.html")

    return df