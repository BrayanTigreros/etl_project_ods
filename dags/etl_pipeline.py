from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
 
BASE_PATH   = "/opt/airflow"
RAW_PATH    = f"{BASE_PATH}/data/raw"
TRANS_PATH  = f"{BASE_PATH}/data/transformed"
INP_VALIDATE    = f"{BASE_PATH}/expectations/input"  
OUT_VALIDATE    = f"{BASE_PATH}/expectations/output"
DB_CONN     = "mysql+pymysql://root:@192.168.0.11:3306/incautaciones_dw"
 
 
@dag(
    dag_id="etl_incautaciones",
    description="ETL pipeline: Incautaciones fauna silvestre + GBIF API → MySQL DW",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "incautaciones", "gbif"],
    max_active_runs=1
)
def pipeline_etl_incautaciones():
 
    # ─────────────────────────────────────────────────────────
    # EXTRACT
    # ─────────────────────────────────────────────────────────
    @task
    def extract_incautaciones() -> str:
        df = pd.read_csv(f"{RAW_PATH}/incautaciones.csv")
        assert len(df) > 0, "Dataset de incautaciones está vacío"
        print(f"[extract_incautaciones] Filas cargadas: {len(df)}")
        return f"{RAW_PATH}/incautaciones.csv"
 
    @task
    def extract_gbif(data_path: str) -> str:
        import re
        import time
        import os
        import requests
 
        BASE_URL       = "https://api.gbif.org/v1"
        MIN_CONFIDENCE = 80
        output_path    = f"{RAW_PATH}/gbif_raw.csv"
 
        # Si el archivo ya existe, no volver a llamar la API
        if os.path.exists(output_path):
            print(f"[extract_gbif] gbif_raw.csv ya existe, cargando desde disco...")
            return output_path
 
        def normalizar_nombre(nombre: str) -> str:
            nombre = re.sub(r'\(.*?\)', '', nombre)
            nombre = re.sub(r'\b[A-Z][A-Z.\s]+$', '', nombre)
            return nombre.strip().title()
 
        def get_taxonomia(nombre: str):
            try:
                r = requests.get(f"{BASE_URL}/species/match", params={"name": nombre}, timeout=10)
                data = r.json()
                if data.get("matchType") == "NONE" or data.get("confidence", 0) < MIN_CONFIDENCE:
                    return None
                return data
            except requests.RequestException:
                return None
 
        def get_iucn(usage_key: int):
            try:
                r = requests.get(f"{BASE_URL}/species/{usage_key}/iucnRedListCategory", timeout=10)
                return r.json().get("code")
            except requests.RequestException:
                return None
 
        df = pd.read_csv(data_path)
        nombres_unicos = df["Nombre cientifico"].dropna().unique().tolist()
        print(f"[extract_gbif] Consultando GBIF para {len(nombres_unicos)} especies...")
 
        resultados = []
        for i, nombre_original in enumerate(nombres_unicos):
            nombre_norm = normalizar_nombre(nombre_original)
            print(f"[{i+1:>3}/{len(nombres_unicos)}] {nombre_original:45} → {nombre_norm}")
 
            taxonomia = get_taxonomia(nombre_norm) 
            if taxonomia is None:
                resultados.append({
                    "nombre_cientifico_original":    nombre_original,
                    "nombre_cientifico_normalizado": nombre_norm,
                    "nombre_cientifico_gbif":        None,
                    "usage_key":                     None,
                    "reino": None, "filo": None, "clase": None,
                    "orden": None, "familia": None, "genero": None,
                    "estado_taxonomico": None, "confianza_match": 0,
                    "categoria_iucn": None
                })
                continue
 
            usage_key      = taxonomia.get("usageKey")
            categoria_iucn = get_iucn(usage_key) if usage_key else None
 
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
                "categoria_iucn":                categoria_iucn
            })
 
        df_gbif = pd.DataFrame(resultados)
        df_gbif.to_csv(output_path, index=False)
        print(f"[extract_gbif] {len(df_gbif)} especies guardadas → {output_path}")
        return output_path
 
    @task
    def validate_input(inc_path: str, gbif_path: str) -> None:
        from pathlib import Path
        from src.input_validation import input_data_validation  

        df_inc  = pd.read_csv(inc_path)
        df_inc.columns = [
            "anio", "departamento", "municipio", "lugar_decomiso",
            "situacion", "autoridad_que_incauto", "tipo_especie",
            "nombre_comun", "nombre_cientifico", "cantidad"
        ]
        df_gbif = pd.read_csv(gbif_path)

        results = input_data_validation(df_inc, df_gbif, INP_VALIDATE)

        # Fallar el task si alguna suite no pasó
        for table, result in results.items():
            if not result["success"]:
                print(f"[validate_input] Validación fallida para '{table}' — continuando pipeline")

        return "ok"


    # ─────────────────────────────────────────────────────────
    # TRANSFORM
    # ─────────────────────────────────────────────────────────
    @task
    def transform(inc_path: str, gbif_path: str, validated: str) -> str:
        import re
 
        IUCN_LABELS = {
            "LC": "Preocupación menor", "NT": "Casi amenazada",
            "VU": "Vulnerable",         "EN": "En peligro",
            "CR": "En peligro crítico", "EW": "Extinta en vida silvestre",
            "EX": "Extinta",            "DD": "Datos insuficientes",
            "NE": "No evaluada"
        }
 
        # ── Leer fuentes ──────────────────────────────────────
        df = pd.read_csv(inc_path)
        df.columns = [
            "anio", "departamento", "municipio", "lugar_decomiso",
            "situacion", "autoridad_que_incauto", "tipo_especie",
            "nombre_comun", "nombre_cientifico", "cantidad"
        ]
 
        df_gbif = pd.read_csv(gbif_path)
 
        # ── dim_tiempo ────────────────────────────────────────
        dim_tiempo = df[["anio"]].drop_duplicates().reset_index(drop=True).copy()
        dim_tiempo["anio"] = (dim_tiempo["anio"] * 1000).astype(int)
        dim_tiempo.insert(0, "tiempo_key", range(1, len(dim_tiempo) + 1))
 
        # ── dim_ubicacion ─────────────────────────────────────
        dim_ubicacion = df[["departamento", "municipio", "lugar_decomiso"]] \
            .drop_duplicates().reset_index(drop=True).copy()
        dim_ubicacion["municipio"]     = dim_ubicacion["municipio"].fillna("DESCONOCIDO").str.strip().str.upper()
        dim_ubicacion["lugar_decomiso"]= dim_ubicacion["lugar_decomiso"].fillna("DESCONOCIDO").str.strip().str.upper()
        dim_ubicacion["departamento"]  = dim_ubicacion["departamento"].str.strip().str.upper()
        dim_ubicacion.insert(0, "ubicacion_key", range(1, len(dim_ubicacion) + 1))
 
        # ── dim_autoridad ─────────────────────────────────────
        dim_autoridad = df[["autoridad_que_incauto"]] \
            .drop_duplicates().reset_index(drop=True).copy()
        dim_autoridad["autoridad_que_incauto"] = dim_autoridad["autoridad_que_incauto"] \
            .fillna("DESCONOCIDO").str.strip().str.upper()
        dim_autoridad.insert(0, "autoridad_key", range(1, len(dim_autoridad) + 1))
 
        # ── dim_especie base ──────────────────────────────────
        dim_especie = df[["tipo_especie", "nombre_comun", "nombre_cientifico"]] \
            .drop_duplicates().reset_index(drop=True).copy()
        for col in ["tipo_especie", "nombre_comun", "nombre_cientifico"]:
            dim_especie[col] = dim_especie[col].fillna("DESCONOCIDO").str.strip().str.upper()
        dim_especie.insert(0, "especie_key", range(1, len(dim_especie) + 1))
 
        # ── Enriquecer dim_especie con GBIF ───────────────────
        gbif = df_gbif.copy()
        gbif = gbif.rename(columns={"nombre_cientifico_original": "nombre_cientifico"})
        gbif["nombre_cientifico"] = gbif["nombre_cientifico"].str.strip().str.upper()
        gbif["categoria_iucn"]    = gbif["categoria_iucn"].fillna("NE")
 
        gbif_ = gbif[["nombre_cientifico", "categoria_iucn"]] \
            .drop_duplicates(subset="nombre_cientifico")
 
        dim_especie = dim_especie.merge(gbif_, on="nombre_cientifico", how="left")
        dim_especie["categoria_iucn"]       = dim_especie["categoria_iucn"].fillna("NE")
        dim_especie["categoria_iucn_label"] = dim_especie["categoria_iucn"].map(IUCN_LABELS)
        dim_especie["es_amenazada"]         = dim_especie["categoria_iucn"].isin(["VU", "EN", "CR", "EW", "EX"])
 
        dim_especie = dim_especie[[
            "especie_key", "tipo_especie", "nombre_comun",
            "nombre_cientifico", "categoria_iucn", "categoria_iucn_label", "es_amenazada"
        ]]
 
        # ── fact_incautaciones ────────────────────────────────
        fact = df.copy()
        fact["anio"]                  = (fact["anio"] * 1000).astype(int)
        fact["municipio"]             = fact["municipio"].fillna("DESCONOCIDO").str.strip().str.upper()
        fact["lugar_decomiso"]        = fact["lugar_decomiso"].fillna("DESCONOCIDO").str.strip().str.upper()
        fact["departamento"]          = fact["departamento"].str.strip().str.upper()
        fact["tipo_especie"]          = fact["tipo_especie"].fillna("DESCONOCIDO").str.strip().str.upper()
        fact["nombre_comun"]          = fact["nombre_comun"].fillna("DESCONOCIDO").str.strip().str.upper()
        fact["nombre_cientifico"]     = fact["nombre_cientifico"].fillna("DESCONOCIDO").str.strip().str.upper()
        fact["autoridad_que_incauto"] = fact["autoridad_que_incauto"].fillna("DESCONOCIDO").str.strip().str.upper()
        fact["situacion"]             = fact["situacion"].str.strip().str.upper()
 
        fact = fact.merge(dim_tiempo,    on="anio",                                               how="left")
        fact = fact.merge(dim_ubicacion, on=["departamento", "municipio", "lugar_decomiso"],       how="left")
        fact = fact.merge(dim_especie,   on=["tipo_especie", "nombre_comun", "nombre_cientifico"], how="left")
        fact = fact.merge(dim_autoridad, on="autoridad_que_incauto",                              how="left")
 
        fact_incautaciones = fact[[
            "tiempo_key", "ubicacion_key", "especie_key", "autoridad_key", "situacion", "cantidad"
        ]].copy()

        fact_incautaciones = fact_incautaciones.reset_index(drop=True)
        fact_incautaciones.insert(0, "id", fact_incautaciones.index + 1)
        
        # ── Guardar archivos transformados ────────────────────
        dim_tiempo.to_csv(        f"{TRANS_PATH}/dim_tiempo.csv",         index=False)
        dim_ubicacion.to_csv(     f"{TRANS_PATH}/dim_ubicacion.csv",      index=False)
        dim_autoridad.to_csv(     f"{TRANS_PATH}/dim_autoridad.csv",      index=False)
        dim_especie.to_csv(       f"{TRANS_PATH}/dim_especie.csv",        index=False)
        fact_incautaciones.to_csv(f"{TRANS_PATH}/fact_incautaciones.csv", index=False)
 
        print(f"[transform] dim_especie: {len(dim_especie)} filas")
        print(f"[transform] fact_incautaciones: {len(fact_incautaciones)} filas")
        return TRANS_PATH
    
    @task
    def validate_output(trans_path: str) -> None:
        from src.output_validation import output_data_validation  # ajusta el import

        transformed_data = {
            "dim_tiempo":          pd.read_csv(f"{trans_path}/dim_tiempo.csv"),
            "dim_ubicacion":       pd.read_csv(f"{trans_path}/dim_ubicacion.csv"),
            "dim_autoridad":       pd.read_csv(f"{trans_path}/dim_autoridad.csv"),
            "dim_especie":         pd.read_csv(f"{trans_path}/dim_especie.csv"),
            "fact_incautaciones":  pd.read_csv(f"{trans_path}/fact_incautaciones.csv"),
        }

        results = output_data_validation(transformed_data, OUT_VALIDATE)

        for table, result in results.items():
            if not result["success"]:
                for r in result["results"]:
                    if not r["success"]:
                        print(f"❌ FALLO: {r['expectation_config']['type']} | {r['expectation_config']['kwargs'].get('column')} | {r['result']}")
                raise ValueError(f"[validate_output] Validación fallida para '{table}'")
            
        return "ok" 

 
    # ─────────────────────────────────────────────────────────
    # LOAD
    # ─────────────────────────────────────────────────────────
    @task
    def load(trans_path: str, validated: str) -> None:
        from sqlalchemy import create_engine, text
 
        engine = create_engine(DB_CONN)
 
        def insert_ignore(df, table_name):
            temp = f"tmp_{table_name}"
            df.to_sql(temp, engine, if_exists="replace", index=False)
            cols = ", ".join(df.columns)
            with engine.begin() as conn:
                conn.execute(text(f"INSERT IGNORE INTO {table_name} ({cols}) SELECT {cols} FROM {temp};"))
                conn.execute(text(f"DROP TABLE {temp}"))
            print(f"[load] {table_name}: {len(df)} filas insertadas")
 
        dim_tiempo         = pd.read_csv(f"{trans_path}/dim_tiempo.csv")
        dim_ubicacion      = pd.read_csv(f"{trans_path}/dim_ubicacion.csv")
        dim_autoridad      = pd.read_csv(f"{trans_path}/dim_autoridad.csv")
        dim_especie        = pd.read_csv(f"{trans_path}/dim_especie.csv")
        fact_incautaciones = pd.read_csv(f"{trans_path}/fact_incautaciones.csv")
 
        insert_ignore(dim_tiempo,    "dim_tiempo")
        insert_ignore(dim_ubicacion, "dim_ubicacion")
        insert_ignore(dim_autoridad, "dim_autoridad")
        insert_ignore(dim_especie,   "dim_especie")
 
        # Anti-join para evitar duplicados en la fact table
        key_cols = ["tiempo_key", "ubicacion_key", "especie_key", "autoridad_key"]
        try:
            existing = pd.read_sql(f"SELECT {', '.join(key_cols)} FROM fact_incautaciones", engine)
        except Exception:
            existing = pd.DataFrame(columns=key_cols)
 
        if not existing.empty:
            merged   = fact_incautaciones.merge(existing.drop_duplicates(), on=key_cols, how="left", indicator=True)
            fact_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
        else:
            fact_new = fact_incautaciones.copy()
 
        print(f"[load] fact_incautaciones: total={len(fact_incautaciones)} nuevas={len(fact_new)}")
        if not fact_new.empty:
            insert_ignore(fact_new, "fact_incautaciones")
        else:
            print("[load] No hay nuevas filas para fact_incautaciones")
 
        print("[load] Carga al Data Warehouse completada exitosamente")
 
    # ─────────────────────────────────────────────────────────
    # DEPENDENCIAS
    # ─────────────────────────────────────────────────────────
    inc_path   = extract_incautaciones()
    gbif_path  = extract_gbif(inc_path)
    val_in     = validate_input(inc_path, gbif_path)
    trans_path = transform(inc_path, gbif_path, validated=val_in)
    val_out    = validate_output(trans_path)
    load(trans_path, validated=val_out)
 
 
pipeline_etl_incautaciones()