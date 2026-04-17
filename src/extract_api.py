from pathlib import Path
import re
import time
import requests
import pandas as pd
from ydata_profiling import ProfileReport

BASE_URL = "https://api.gbif.org/v1"
MIN_CONFIDENCE = 80


def normalizar_nombre(nombre: str) -> str:
    nombre = re.sub(r'\(.*?\)', '', nombre)
    nombre = nombre.strip()

    palabras = nombre.split()

    if len(palabras) >= 2:
        genero = palabras[0].capitalize()
        especie = palabras[1].lower()
        return f"{genero} {especie}"

    return nombre.title()

def es_nombre_cientifico(nombre: str) -> bool:
    palabras = nombre.strip().split()

    if len(palabras) != 2:
        return False

    genero, especie = palabras

    return (
        genero[0].isupper() and genero[1:].islower() and
        especie.islower()
    )


def get_taxonomia(nombre: str) -> dict:
    try:
        r = requests.get(f"{BASE_URL}/species/match", params={"name": nombre}, timeout=10)
        data = r.json()

        if data.get("matchType") == "NONE" or data.get("confidence", 0) < MIN_CONFIDENCE:
            return None

        return data
    except requests.RequestException:
        return None


def get_iucn(usage_key: int) -> str:
    try:
        r = requests.get(f"{BASE_URL}/species/{usage_key}/iucnRedListCategory", timeout=10)
        return r.json().get("code")
    except requests.RequestException:
        return None


def extract_gbif(data_path: str, output_path: str, force_refresh: bool = False) -> pd.DataFrame:

    output_file = Path(output_path)

    if output_file.exists() and not force_refresh:
        print(f"gbif_raw.csv ya existe. Leyendo desde disco: {output_path}")
        return pd.read_csv(output_file)

    df = pd.read_csv(data_path)
    nombres_unicos = df["Nombre cientifico"].dropna().unique().tolist()
    print(f"Consultando GBIF para {len(nombres_unicos)} especies...")

    resultados = []
    sin_match = 0
    no_cientificos = 0

    for i, nombre_original in enumerate(nombres_unicos):
        nombre_norm = normalizar_nombre(nombre_original)

        print(f"[{i+1:>3}/{len(nombres_unicos)}] {nombre_original:45} → {nombre_norm}")

        #filtro nombres basura
        if not es_nombre_cientifico(nombre_norm):
            no_cientificos += 1

            resultados.append({
                "nombre_cientifico_original": nombre_original,
                "nombre_cientifico_normalizado": nombre_norm,
                "nombre_cientifico_gbif": None,
                "usage_key": None,
                "reino": None,
                "filo": None,
                "clase": None,
                "orden": None,
                "familia": None,
                "genero": None,
                "estado_taxonomico": None,
                "confianza_match": 0,
                "categoria_iucn": None
            })
            continue

        taxonomia = get_taxonomia(nombre_norm)

        #log de fallos
        if taxonomia is None:
            sin_match += 1
            print(f"Aviso: No match o baja confianza")

            resultados.append({
                "nombre_cientifico_original": nombre_original,
                "nombre_cientifico_normalizado": nombre_norm,
                "nombre_cientifico_gbif": None,
                "usage_key": None,
                "reino": None,
                "filo": None,
                "clase": None,
                "orden": None,
                "familia": None,
                "genero": None,
                "estado_taxonomico": None,
                "confianza_match": 0,
                "categoria_iucn": None
            })
            continue

        usage_key = taxonomia.get("usageKey")

        #solo llamar IUCN si el match es fuerte
        if usage_key and taxonomia.get("confidence", 0) >= 90:
            categoria_iucn = get_iucn(usage_key)
        else:
            categoria_iucn = None

        resultados.append({
            "nombre_cientifico_original": nombre_original,
            "nombre_cientifico_normalizado": nombre_norm,
            "nombre_cientifico_gbif": taxonomia.get("scientificName"),
            "usage_key": usage_key,
            "reino": taxonomia.get("kingdom"),
            "filo": taxonomia.get("phylum"),
            "clase": taxonomia.get("class"),
            "orden": taxonomia.get("order"),
            "familia": taxonomia.get("family"),
            "genero": taxonomia.get("genus"),
            "estado_taxonomico": taxonomia.get("status"),
            "confianza_match": taxonomia.get("confidence"),
            "categoria_iucn": categoria_iucn
        })


    df_gbif = pd.DataFrame(resultados)
    df_gbif.to_csv(output_file, index=False)

    print("\n=== RESUMEN GBIF ===")
    print(f"Total especies           : {len(nombres_unicos)}")
    print(f"Sin match                : {sin_match}")
    print(f"No científicos           : {no_cientificos}")
    print(f"Con match válido         : {len(nombres_unicos) - sin_match - no_cientificos}")

    print(f"\nGBIF extraído: {len(df_gbif)} especies → {output_path}")

    return df_gbif

def profiling_api(df_gbif):
    profile = ProfileReport(df_gbif, title="Data Profiling Report API", explorative=True)

    # Or save the report to an HTML file
    profile.to_file(r"C:\Users\santa\Desktop\ETL_cositas\proyecto_etl_ods\profiling\api_profiling_report.html")

    return df_gbif