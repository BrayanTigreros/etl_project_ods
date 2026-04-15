import re
import time
import requests
import pandas as pd

BASE_URL = "https://api.gbif.org/v1"
SLEEP_TIME = 0.3
MIN_CONFIDENCE = 80

def normalizar_nombre(nombre: str) -> str:
    nombre = re.sub(r'\(.*?\)', '', nombre)
    nombre = re.sub(r'\b[A-Z][A-Z.\s]+$', '', nombre)
    return nombre.strip().title()

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

def extract_gbif(data_path: str, output_path: str) -> pd.DataFrame:
    df = pd.read_csv(data_path)
    nombres_unicos = df["Nombre cientifico"].dropna().unique().tolist()
    print(f"Consultando GBIF para {len(nombres_unicos)} especies...")

    resultados = []
    for i, nombre_original in enumerate(nombres_unicos):
        nombre_norm = normalizar_nombre(nombre_original)
        print(f"[{i+1:>3}/{len(nombres_unicos)}] {nombre_original:45} → {nombre_norm}")

        taxonomia = get_taxonomia(nombre_norm)
        time.sleep(SLEEP_TIME)

        if taxonomia is None:
            resultados.append({
                "nombre_cientifico_original": nombre_original,
                "nombre_cientifico_normalizado": nombre_norm,
                "nombre_cientifico_gbif": None, "usage_key": None,
                "reino": None, "filo": None, "clase": None,
                "orden": None, "familia": None, "genero": None,
                "estado_taxonomico": None, "confianza_match": 0,
                "categoria_iucn": None
            })
            continue

        usage_key = taxonomia.get("usageKey")
        categoria_iucn = get_iucn(usage_key) if usage_key else None
        time.sleep(SLEEP_TIME)

        resultados.append({
            "nombre_cientifico_original":    nombre_original,
            "nombre_cientifico_normalizado": nombre_norm,
            "nombre_cientifico_gbif":        taxonomia.get("scientificName"),
            "usage_key":                     usage_key,
            "reino":                         taxonomia.get("kingdom"),
            "filo":                          taxonomia.get("phylum"),
            "clase":                         taxonomia.get("class"),
            "orden":                         taxonomia.get("order"),
            "familia":                       taxonomia.get("family"),
            "genero":                        taxonomia.get("genus"),
            "estado_taxonomico":             taxonomia.get("status"),
            "confianza_match":               taxonomia.get("confidence"),
            "categoria_iucn":               categoria_iucn
        })

    df_gbif = pd.DataFrame(resultados)
    df_gbif.to_csv(output_path, index=False)
    print(f"\nGBIF extraído: {len(df_gbif)} especies → {output_path}")
    return df_gbif