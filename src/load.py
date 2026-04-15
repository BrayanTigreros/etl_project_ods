from sqlalchemy import create_engine, text
import pandas as pd
import os


# Save to csv
def save_dimensions_to_csv(target_file, **dataframes):
    os.makedirs(target_file, exist_ok=True)

    for name, df in dataframes.items():
        file_path = os.path.join(target_file, f"{name}.csv")
        df.to_csv(file_path, index=False)
        print(f"Saved: {file_path}")


def insert_ignore(df, table_name, engine):
    """
    Inserta evitando duplicados.
    Útil para dimensiones que no cambian o para facts.
    """
    temp_table = f"tmp_{table_name}"
    cols = ", ".join(df.columns)

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    insert_sql = f"""
        INSERT IGNORE INTO {table_name} ({cols})
        SELECT {cols}
        FROM {temp_table};
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))


def load_dim_especie(dim_especie, engine):
    """
    Mantiene INSERT IGNORE para no duplicar, pero luego actualiza
    por especie_key los campos que sí pueden cambiar.
    """
    temp_table = "tmp_dim_especie"
    cols = ", ".join(dim_especie.columns)

    dim_especie.to_sql(temp_table, engine, if_exists="replace", index=False)

    insert_sql = f"""
        INSERT IGNORE INTO dim_especie ({cols})
        SELECT {cols}
        FROM {temp_table};
    """

    update_sql = f"""
        UPDATE dim_especie d
        JOIN {temp_table} t
            ON d.especie_key = t.especie_key
        SET
            d.tipo_especie = t.tipo_especie,
            d.nombre_comun = t.nombre_comun,
            d.nombre_cientifico = t.nombre_cientifico,
            d.categoria_iucn = t.categoria_iucn,
            d.categoria_iucn_label = t.categoria_iucn_label,
            d.es_amenazada = t.es_amenazada;
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(update_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))


# Load to DW
def load_to_dw(dataframes):
    dim_tiempo = dataframes["dim_tiempo"]
    dim_ubicacion = dataframes["dim_ubicacion"]
    dim_especie = dataframes["dim_especie"]
    dim_autoridad = dataframes["dim_autoridad"]
    fact_incautaciones = dataframes["fact_incautaciones"]

    engine = create_engine(
        "mysql+pymysql://root:davidsp@localhost:3306/incautaciones_dw"
    )

    insert_ignore(dim_tiempo, "dim_tiempo", engine)
    insert_ignore(dim_ubicacion, "dim_ubicacion", engine)
    load_dim_especie(dim_especie, engine)
    insert_ignore(dim_autoridad, "dim_autoridad", engine)

    # Anti-join para evitar duplicados en la fact table
    key_cols = [
        "tiempo_key",
        "ubicacion_key",
        "especie_key",
        "autoridad_key",
    ]

    existing_keys_sql = f"SELECT {', '.join(key_cols)} FROM fact_incautaciones"

    try:
        existing_keys_df = pd.read_sql(existing_keys_sql, engine)
    except Exception:
        existing_keys_df = pd.DataFrame(columns=key_cols)

    if not existing_keys_df.empty:
        merged = fact_incautaciones.merge(
            existing_keys_df.drop_duplicates(),
            on=key_cols,
            how="left",
            indicator=True,
        )
        fact_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        fact_new = fact_incautaciones.copy()

    print(
        f"Fact total={len(fact_incautaciones)} "
        f"new rows={len(fact_new)} "
        f"duplicates omitted={len(fact_incautaciones) - len(fact_new)}"
    )

    if not fact_new.empty:
        insert_ignore(fact_new, "fact_incautaciones", engine)
    else:
        print("No hay nuevas filas para fact_incautaciones")

    print("Carga al Data Warehouse completada exitosamente")