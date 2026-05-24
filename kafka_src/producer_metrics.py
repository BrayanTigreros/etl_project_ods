import json
import time
import pandas as pd
from kafka import KafkaProducer
from sqlalchemy import create_engine, text

TOPIC     = "incautaciones-metrics"
BROKER    = "localhost:9092"
DB_CONN   = "mysql+pymysql://root:@192.168.1.5:3306/incautaciones_dw"
DELAY_SEC = 30

QUERY_POR_ANIO = text("""
    SELECT
        t.anio          AS anio,
        SUM(f.cantidad) AS total_individuos,
        COUNT(*)        AS total_eventos
    FROM fact_incautaciones f
    JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
    GROUP BY t.anio
    ORDER BY t.anio
""")

QUERY_POR_AMENAZA = text("""
    SELECT
        e.categoria_iucn_label AS nivel_amenaza,
        e.categoria_iucn       AS codigo_iucn,
        SUM(f.cantidad)        AS total_individuos,
        COUNT(*)               AS total_eventos
    FROM fact_incautaciones f
    JOIN dim_especie e ON f.especie_key = e.especie_key
    GROUP BY e.categoria_iucn_label, e.categoria_iucn
    ORDER BY total_individuos DESC
""")

QUERY_POR_AUTORIDAD = text("""
    SELECT
        a.autoridad_que_incauto AS autoridad,
        COUNT(*)                AS total_eventos,
        SUM(f.cantidad)         AS total_individuos
    FROM fact_incautaciones f
    JOIN dim_autoridad a ON f.autoridad_key = a.autoridad_key
    GROUP BY a.autoridad_que_incauto
    ORDER BY total_individuos DESC
    LIMIT 10
""")


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )


def fetch_metrics(engine):
    with engine.connect() as conn:
        df_anio      = pd.read_sql(QUERY_POR_ANIO,      conn)
        df_amenaza   = pd.read_sql(QUERY_POR_AMENAZA,   conn)
        df_autoridad = pd.read_sql(QUERY_POR_AUTORIDAD, conn)
    return {
        "por_anio":      df_anio.to_dict(orient="records"),
        "por_amenaza":   df_amenaza.to_dict(orient="records"),
        "por_autoridad": df_autoridad.to_dict(orient="records"),
    }


def publish_metrics(producer, metrics):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    for metric_type, records in metrics.items():
        message = {
            "metric_type": metric_type,
            "timestamp":   timestamp,
            "total_rows":  len(records),
            "data":        records,
        }
        producer.send(TOPIC, value=message)
        print(f"  [producer] '{metric_type}' → {len(records)} registros publicados")
    producer.flush()


def run_producer(iterations=None):
    engine   = create_engine(DB_CONN)
    producer = build_producer()
    print(f"[producer] Conectado. Topic: '{TOPIC}' | Intervalo: {DELAY_SEC}s\n")

    ronda = 0
    try:
        while iterations is None or ronda < iterations:
            ronda += 1
            print(f"[producer] ── Ronda {ronda} ── {time.strftime('%H:%M:%S')}")
            try:
                metrics = fetch_metrics(engine)
                publish_metrics(producer, metrics)
            except Exception as e:
                print(f"[producer] ERROR: {e}")
            if iterations is None or ronda < iterations:
                time.sleep(DELAY_SEC)
    except KeyboardInterrupt:
        print("\n[producer] Detenido manualmente.")
    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()