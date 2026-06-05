"""
init_data.py
────────────
Script de inicialización que se ejecuta una sola vez al arrancar:
  1. Crea el keyspace y las tablas en Cassandra
  2. Importa los datos de distancias desde el .jsonl a Cassandra
  3. Sube el CSV de vuelos a MinIO (para Iceberg en el entrenamiento)

Se espera que Cassandra y MinIO ya estén healthy (docker-compose
lo garantiza con depends_on + healthcheck).
"""

import json
import os
import sys
import time
import boto3
from botocore.client import Config
from cassandra.cluster import Cluster
from cassandra.policies import RetryPolicy

# ── Configuración ──────────────────────────────────────────────────────────────
CASSANDRA_HOST   = os.getenv("CASSANDRA_HOST", "cassandra")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

DISTANCES_JSONL  = "/app/data/origin_dest_distances.jsonl"   # generado por import_distances.sh original
FLIGHTS_CSV_DIR  = "/app/data"                    # directorio con los CSV de vuelos
MINIO_BUCKET_DATA   = "flights-data"
MINIO_BUCKET_MODELS = "models"

# ── 1. Conectar a Cassandra (con reintentos) ────────────────────────────────────
def connect_cassandra(host, retries=20, delay=5):
    for i in range(retries):
        try:
            cluster = Cluster([host], default_retry_policy=RetryPolicy())
            session = cluster.connect()
            print(f"✅ Conectado a Cassandra en {host}")
            return cluster, session
        except Exception as e:
            print(f"⏳ Esperando Cassandra ({i+1}/{retries}): {e}")
            time.sleep(delay)
    print("❌ No se pudo conectar a Cassandra")
    sys.exit(1)

# ── 2. Crear keyspace y tablas ─────────────────────────────────────────────────
def setup_cassandra(session):
    # Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS agile_data_science
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("agile_data_science")
    print("✅ Keyspace 'agile_data_science' listo")

    # Tabla de distancias (equivalente a la colección MongoDB)
    session.execute("""
        CREATE TABLE IF NOT EXISTS origin_dest_distances (
            origin   TEXT,
            dest     TEXT,
            distance DOUBLE,
            PRIMARY KEY (origin, dest)
        )
    """)
    print("✅ Tabla 'origin_dest_distances' lista")

    # Tabla de predicciones (resultado del job de Spark)
    session.execute("""
        CREATE TABLE IF NOT EXISTS flight_delay_ml_response (
            uuid        TEXT PRIMARY KEY,
            origin      TEXT,
            dest        TEXT,
            carrier     TEXT,
            flight_date TEXT,
            dep_delay   DOUBLE,
            distance    DOUBLE,
            prediction  INT,
            timestamp   TIMESTAMP
        )
    """)
    print("✅ Tabla 'flight_delay_ml_response' lista")

# ── 3. Importar distancias ─────────────────────────────────────────────────────
def import_distances(session):
    if not os.path.exists(DISTANCES_JSONL):
        print(f"⚠️  No se encontró {DISTANCES_JSONL}, omitiendo importación de distancias")
        return

    insert_stmt = session.prepare("""
        INSERT INTO origin_dest_distances (origin, dest, distance)
        VALUES (?, ?, ?)
    """)

    count = 0
    with open(DISTANCES_JSONL) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            origin   = doc.get("Origin", doc.get("origin", ""))
            dest     = doc.get("Dest", doc.get("dest", ""))
            distance = float(doc.get("Distance", doc.get("distance", 0)))
            session.execute(insert_stmt, (origin, dest, distance))
            count += 1

    print(f"✅ {count} distancias importadas a Cassandra")

# ── 4. Subir CSV de vuelos a MinIO ─────────────────────────────────────────────
def upload_flights_to_minio():
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
        )

        # Crear buckets si no existen
        for bucket in [MINIO_BUCKET_DATA, MINIO_BUCKET_MODELS]:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f"✅ Bucket '{bucket}' creado")
            except s3.exceptions.BucketAlreadyOwnedByYou:
                print(f"ℹ️  Bucket '{bucket}' ya existe")

        # Subir todos los CSV del directorio de datos
        csv_files = [f for f in os.listdir(FLIGHTS_CSV_DIR) if f.endswith(".csv")]
        if not csv_files:
            print(f"⚠️  No hay CSV en {FLIGHTS_CSV_DIR}, omitiendo subida a MinIO")
            return

        for csv_file in csv_files:
            local_path = os.path.join(FLIGHTS_CSV_DIR, csv_file)
            s3_key     = f"flights/{csv_file}"
            s3.upload_file(local_path, MINIO_BUCKET_DATA, s3_key)
            print(f"✅ Subido {csv_file} → s3://{MINIO_BUCKET_DATA}/{s3_key}")

    except Exception as e:
        print(f"⚠️  Error subiendo datos a MinIO: {e}")
        print("   (Continúa sin MinIO, se puede subir manualmente)")

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Iniciando setup de datos...")

    cluster, session = connect_cassandra(CASSANDRA_HOST)
    setup_cassandra(session)
    import_distances(session)
    cluster.shutdown()

    upload_flights_to_minio()

    print("✅ Inicialización completada")
