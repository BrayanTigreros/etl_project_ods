import json
from kafka import KafkaConsumer

TOPIC    = "incautaciones-metrics"
BROKER   = "localhost:9092"
GROUP_ID = "incautaciones-consumer-group"


def print_header(metric_type, timestamp, total_rows):
    print(f"\n{'='*60}")
    print(f"  MÉTRICA  : {metric_type.upper().replace('_', ' ')}")
    print(f"  HORA     : {timestamp}")
    print(f"  REGISTROS: {total_rows}")
    print(f"{'='*60}")


def format_por_anio(records):
    print(f"  {'AÑO':<8} {'INDIVIDUOS':>12} {'EVENTOS':>10}")
    print(f"  {'-'*32}")
    for r in records:
        print(f"  {int(r['anio']):<8} {int(r['total_individuos']):>12,} {int(r['total_eventos']):>10,}")


def format_por_amenaza(records):
    print(f"  {'NIVEL DE AMENAZA':<30} {'CÓDIGO':<8} {'INDIVIDUOS':>12}")
    print(f"  {'-'*52}")
    for r in records:
        label = str(r.get("nivel_amenaza") or "Sin categoría")[:29]
        print(f"  {label:<30} {r.get('codigo_iucn',''):<8} {int(r['total_individuos']):>12,}")


def format_por_autoridad(records):
    print(f"  {'AUTORIDAD':<45} {'INDIVIDUOS':>12} {'EVENTOS':>8}")
    print(f"  {'-'*67}")
    for r in records:
        autoridad = str(r.get("autoridad") or "DESCONOCIDA")[:44]
        print(f"  {autoridad:<45} {int(r['total_individuos']):>12,} {int(r['total_eventos']):>8,}")


FORMATTERS = {
    "por_anio":      format_por_anio,
    "por_amenaza":   format_por_amenaza,
    "por_autoridad": format_por_autoridad,
}


def process_message(message):
    metric_type = message.get("metric_type", "desconocida")
    print_header(metric_type, message.get("timestamp", ""), message.get("total_rows", 0))
    formatter = FORMATTERS.get(metric_type)
    if formatter:
        formatter(message.get("data", []))
    print()


def run_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    print(f"[consumer] Escuchando '{TOPIC}'... (Ctrl+C para detener)\n")
    try:
        for message in consumer:
            process_message(message.value)
    except KeyboardInterrupt:
        print("\n[consumer] Detenido manualmente.")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()