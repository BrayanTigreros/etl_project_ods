import pandas as pd

#mapeado de categorias IUCN para etiquetas legibles
IUCN_LABELS = {
    "LC": "Preocupación menor",
    "NT": "Casi amenazada",
    "VU": "Vulnerable",
    "EN": "En peligro",
    "CR": "En peligro crítico",
    "EW": "Extinta en vida silvestre",
    "EX": "Extinta",
    "DD": "Datos insuficientes",
    "NE": "No evaluada"
}
 
#orden para clasificación de categorías IUCN, de menos a mas grave
IUCN_ORDEN = {
    "LC": 1, "NT": 2, "VU": 3, "EN": 4,
    "CR": 5, "EW": 6, "EX": 7, "DD": 8, "NE": 9
}
 
 
# 
def nueva_dim_especie(dim_especie: pd.DataFrame, df_gbif: pd.DataFrame) -> pd.DataFrame:
    """
    Recibe:
      - dim_especie: la dimensión ya construida en transform.py
      - df_gbif:     el DataFrame crudo de GBIF (gbif_raw.csv)
 
    Retorna:
      - nueva_dim_especie con columnas taxonómicas e IUCN
    """
 
    #Preparar tabla nueva desde GBIF
    gbif = df_gbif.copy()
 
    #nombre_cientifico_original ya viene en mayúsculas
    #al igual que nombre_cientifico en dim_especie
    gbif = gbif.rename(columns={
        "nombre_cientifico_original": "nombre_cientifico"
    })
 
    # Rellenar nulos antes del join
    gbif["reino"]   = gbif["reino"].fillna("NO IDENTIFICADO")
    gbif["filo"]    = gbif["filo"].fillna("NO IDENTIFICADO")
    gbif["clase"]   = gbif["clase"].fillna("NO IDENTIFICADO")
    gbif["orden"]   = gbif["orden"].fillna("NO IDENTIFICADO")
    gbif["familia"] = gbif["familia"].fillna("NO IDENTIFICADO")
    gbif["genero"]  = gbif["genero"].fillna("NO IDENTIFICADO")
    gbif["categoria_iucn"] = gbif["categoria_iucn"].fillna("NE")
 
    # Estandarizar a mayúsculas para que el join no falle por capitalización
    gbif["nombre_cientifico"] = gbif["nombre_cientifico"].str.strip().str.upper()
    gbif["reino"]   = gbif["reino"].str.strip().str.upper()
    gbif["filo"]    = gbif["filo"].str.strip().str.upper()
    gbif["clase"]   = gbif["clase"].str.strip().str.upper()
    gbif["orden"]   = gbif["orden"].str.strip().str.upper()
    gbif["familia"] = gbif["familia"].str.strip().str.upper()
    gbif["genero"]  = gbif["genero"].str.strip().str.upper()
 
    # Quedarnos solo con las columnas útiles para el enriquecimiento
    gbif_ = gbif[[
        "nombre_cientifico",
        "reino",
        "filo",
        "clase",
        "orden",
        "familia",
        "genero",
        "categoria_iucn"
    ]].drop_duplicates(subset="nombre_cientifico")
 
    #LEFT JOIN sobre dim_especie porque queremos conservar todas las filas de dim_especie
    # aunque no haya match en GBIF
    dim_nueva = dim_especie.merge(
        gbif_,
        on="nombre_cientifico",
        how="left"
    )
 
    #Rellenar nulos de especies
    # Las filas que no matchearon quedan = "DESCONOCIDO"
    for col in ["reino", "filo", "clase", "orden", "familia", "genero"]:
        dim_nueva[col] = dim_nueva[col].fillna("NO IDENTIFICADO")
 
    dim_nueva["categoria_iucn"] = dim_nueva["categoria_iucn"].fillna("NE")
 
    #agregar columnas derivadas
    dim_nueva["categoria_iucn_label"] = dim_nueva["categoria_iucn"].map(IUCN_LABELS)
    dim_nueva["iucn_orden"]           = dim_nueva["categoria_iucn"].map(IUCN_ORDEN)
    dim_nueva["es_amenazada"]         = dim_nueva["categoria_iucn"].isin(["VU", "EN", "CR", "EW", "EX"])
 
    print("=== dim_especie enriquecida ===")
    print(f"Total filas            : {len(dim_nueva)}")
    print(f"Con match GBIF         : {(dim_nueva['reino'] != 'NO IDENTIFICADO').sum()}")
    print(f"Especies amenazadas    : {dim_nueva['es_amenazada'].sum()}")
    print(f"\nDistribución IUCN:")
    print(
        dim_nueva
        .groupby(["categoria_iucn", "categoria_iucn_label"])
        .size()
        .reset_index(name="count")
        .sort_values("categoria_iucn")
        .to_string(index=False)
    )
 
    return dim_nueva