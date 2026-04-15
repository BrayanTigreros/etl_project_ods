import pandas as pd
from transform_api import nueva_dim_especie

# Dim tiempo
def create_dim_tiempo(df):

    dim_tiempo = df[["anio"]].drop_duplicates().reset_index(drop=True).copy()

    # El año viene como float (2.008 = 2008), se corrige multiplicando por 1000
    dim_tiempo["anio"] = (dim_tiempo["anio"] * 1000).astype(int)

    dim_tiempo["tiempo_key"] = dim_tiempo.index + 1

    dim_tiempo = dim_tiempo[["tiempo_key", "anio"]]

    return dim_tiempo


# Dim ubicacion
def create_dim_ubicacion(df):

    dim_ubicacion = df[["departamento", "municipio", "lugar_decomiso"]] \
        .drop_duplicates().reset_index(drop=True).copy()

    # Rellenar nulos
    dim_ubicacion["municipio"] = dim_ubicacion["municipio"].fillna("DESCONOCIDO")
    dim_ubicacion["lugar_decomiso"] = dim_ubicacion["lugar_decomiso"].fillna("DESCONOCIDO")

    # Estandarizar a mayúsculas
    dim_ubicacion["departamento"] = dim_ubicacion["departamento"].str.strip().str.upper()
    dim_ubicacion["municipio"] = dim_ubicacion["municipio"].str.strip().str.upper()
    dim_ubicacion["lugar_decomiso"] = dim_ubicacion["lugar_decomiso"].str.strip().str.upper()

    dim_ubicacion["ubicacion_key"] = dim_ubicacion.index + 1

    dim_ubicacion = dim_ubicacion[["ubicacion_key", "departamento", "municipio", "lugar_decomiso"]]

    return dim_ubicacion


# Dim especie
def create_dim_especie(df):

    dim_especie = df[["tipo_especie", "nombre_comun", "nombre_cientifico"]] \
        .drop_duplicates().reset_index(drop=True).copy()

    # Rellenar nulos
    dim_especie["tipo_especie"] = dim_especie["tipo_especie"].fillna("DESCONOCIDO")
    dim_especie["nombre_comun"] = dim_especie["nombre_comun"].fillna("DESCONOCIDO")
    dim_especie["nombre_cientifico"] = dim_especie["nombre_cientifico"].fillna("DESCONOCIDO")

    # Estandarizar a mayúsculas
    dim_especie["tipo_especie"] = dim_especie["tipo_especie"].str.strip().str.upper()
    dim_especie["nombre_comun"] = dim_especie["nombre_comun"].str.strip().str.upper()
    dim_especie["nombre_cientifico"] = dim_especie["nombre_cientifico"].str.strip().str.upper()

    dim_especie = dim_especie.sort_values("nombre_cientifico").reset_index(drop=True)
    dim_especie["especie_key"] = dim_especie.index + 1

    dim_especie = dim_especie[["especie_key", "tipo_especie", "nombre_comun", "nombre_cientifico"]]

    return dim_especie


# Dim autoridad
def create_dim_autoridad(df):

    dim_autoridad = df[["autoridad_que_incauto"]] \
        .drop_duplicates().reset_index(drop=True).copy()

    # Rellenar nulos
    dim_autoridad["autoridad_que_incauto"] = dim_autoridad["autoridad_que_incauto"].fillna("DESCONOCIDO")

    # Estandarizar
    dim_autoridad["autoridad_que_incauto"] = dim_autoridad["autoridad_que_incauto"].str.strip().str.upper()

    dim_autoridad["autoridad_key"] = dim_autoridad.index + 1

    dim_autoridad = dim_autoridad[["autoridad_key", "autoridad_que_incauto"]]

    return dim_autoridad


# Fact table
def create_fact_incautaciones(df, dim_tiempo, dim_ubicacion, dim_especie, dim_autoridad):

    fact = df.copy()

    # Corregir año para hacer el merge con dim_tiempo
    fact["anio"] = (fact["anio"] * 1000).astype(int)

    # Estandarizar columnas para el merge
    fact["municipio"] = fact["municipio"].fillna("DESCONOCIDO").str.strip().str.upper()
    fact["lugar_decomiso"] = fact["lugar_decomiso"].fillna("DESCONOCIDO").str.strip().str.upper()
    fact["departamento"] = fact["departamento"].str.strip().str.upper()
    fact["tipo_especie"] = fact["tipo_especie"].fillna("DESCONOCIDO").str.strip().str.upper()
    fact["nombre_comun"] = fact["nombre_comun"].fillna("DESCONOCIDO").str.strip().str.upper()
    fact["nombre_cientifico"] = fact["nombre_cientifico"].fillna("DESCONOCIDO").str.strip().str.upper()
    fact["autoridad_que_incauto"] = fact["autoridad_que_incauto"].fillna("DESCONOCIDO").str.strip().str.upper()
    fact["situacion"] = fact["situacion"].str.strip().str.upper()

    # Merges con dimensiones
    fact = fact.merge(dim_tiempo, on="anio", how="left")
    fact = fact.merge(dim_ubicacion, on=["departamento", "municipio", "lugar_decomiso"], how="left")
    fact = fact.merge(dim_especie, on=["tipo_especie", "nombre_comun", "nombre_cientifico"], how="left")
    fact = fact.merge(dim_autoridad, on="autoridad_que_incauto", how="left")

    fact_incautaciones = fact[[
        "tiempo_key",
        "ubicacion_key",
        "especie_key",
        "autoridad_key",
        "situacion",
        "cantidad"
    ]].copy()

    return fact_incautaciones


# Transform completo
def transform_data(df, df_gbif):

    dim_tiempo = create_dim_tiempo(df)
    dim_ubicacion = create_dim_ubicacion(df)
    dim_especie = create_dim_especie(df)
    dim_autoridad = create_dim_autoridad(df)
    dim_especie = nueva_dim_especie(dim_especie, df_gbif)

    fact_incautaciones = create_fact_incautaciones(
        df,
        dim_tiempo,
        dim_ubicacion,
        dim_especie,
        dim_autoridad
    )

    return {
        "dim_tiempo": dim_tiempo,
        "dim_ubicacion": dim_ubicacion,
        "dim_especie": dim_especie,
        "dim_autoridad": dim_autoridad,
        "fact_incautaciones": fact_incautaciones
    }
