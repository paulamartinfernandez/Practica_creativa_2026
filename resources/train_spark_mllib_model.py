# !/usr/bin/env python

import sys, os

def main(base_path):

  try: base_path
  except NameError: base_path = "."
  if not base_path:
    base_path = "."

  APP_NAME = "train_spark_mllib_model.py"

  # ── Configuración MinIO ──────────────────────────────────────────────────────
  MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
  MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
  MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
  BUCKET_DATA      = os.environ.get("MINIO_BUCKET_DATA",   "flights-data")
  BUCKET_MODELS    = os.environ.get("MINIO_BUCKET_MODELS", "models")

  # Rutas S3 para datos y modelos
  input_path  = "s3a://{}/flights/simple_flight_delay_features.jsonl.bz2".format(BUCKET_DATA)
  models_path = "s3a://{}".format(BUCKET_MODELS)

  # ── SparkSession con soporte S3/MinIO e Iceberg ───────────────────────────────
  from pyspark.sql import SparkSession

  spark = SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access",      "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse",
            "s3a://{}/iceberg".format(BUCKET_DATA)) \
    .getOrCreate()

  from pyspark.sql.types import (StringType, IntegerType, DoubleType,
                                  DateType, TimestampType,
                                  StructType, StructField)
  from pyspark.sql.functions import udf, lit, concat

  schema = StructType([
    StructField("ArrDelay",   DoubleType(),    True),
    StructField("CRSArrTime", TimestampType(), True),
    StructField("CRSDepTime", TimestampType(), True),
    StructField("Carrier",    StringType(),    True),
    StructField("DayOfMonth", IntegerType(),   True),
    StructField("DayOfWeek",  IntegerType(),   True),
    StructField("DayOfYear",  IntegerType(),   True),
    StructField("DepDelay",   DoubleType(),    True),
    StructField("Dest",       StringType(),    True),
    StructField("Distance",   DoubleType(),    True),
    StructField("FlightDate", DateType(),      True),
    StructField("FlightNum",  StringType(),    True),
    StructField("Origin",     StringType(),    True),
  ])

  # ── Leer datos desde MinIO (S3a) ──────────────────────────────────────────────
  print("Leyendo datos desde: {}".format(input_path))
  features = spark.read.json(input_path, schema=schema)
  features.first()

  # Comprobar nulos
  null_counts = [(col, features.where(features[col].isNull()).count())
                 for col in features.columns]
  cols_with_nulls = list(filter(lambda x: x[1] > 0, null_counts))
  print("Columnas con nulos:", cols_with_nulls)

  # Añadir columna Route
  features_with_route = features.withColumn(
    'Route',
    concat(features.Origin, lit('-'), features.Dest)
  )
  features_with_route.show(6)

  # ── Bucketizer ────────────────────────────────────────────────────────────────
  from pyspark.ml.feature import Bucketizer

  splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
  arrival_bucketizer = Bucketizer(
    splits=splits,
    inputCol="ArrDelay",
    outputCol="ArrDelayBucket"
  )

  # Guardar bucketizer en MinIO
  arrival_bucketizer_path = "{}/arrival_bucketizer_2.0.bin".format(models_path)
  arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)
  print("Bucketizer guardado en:", arrival_bucketizer_path)

  ml_bucketized_features = arrival_bucketizer.transform(features_with_route)

  # ── StringIndexer para campos categóricos ────────────────────────────────────
  from pyspark.ml.feature import StringIndexer, VectorAssembler

  for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
    string_indexer_model = string_indexer.fit(ml_bucketized_features)
    ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
    ml_bucketized_features = ml_bucketized_features.drop(column)

    # Guardar en MinIO
    indexer_path = "{}/string_indexer_model_{}.bin".format(models_path, column)
    string_indexer_model.write().overwrite().save(indexer_path)
    print("StringIndexer {} guardado en: {}".format(column, indexer_path))

  # ── VectorAssembler ───────────────────────────────────────────────────────────
  numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
  index_columns   = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]

  vector_assembler = VectorAssembler(
    inputCols=numeric_columns + index_columns,
    outputCol="Features_vec"
  )
  final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

  # Guardar en MinIO
  assembler_path = "{}/numeric_vector_assembler.bin".format(models_path)
  vector_assembler.write().overwrite().save(assembler_path)
  print("VectorAssembler guardado en:", assembler_path)

  for column in index_columns:
    final_vectorized_features = final_vectorized_features.drop(column)

  final_vectorized_features.show()

# ── Entrenar RandomForest ─────────────────────────────────────────────────────
  from pyspark.ml.classification import RandomForestClassifier
  import mlflow

  mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
  mlflow.set_experiment("flight_delay_prediction")
  mlflow.start_run()

  rfc = RandomForestClassifier(
    featuresCol="Features_vec",
    labelCol="ArrDelayBucket",
    predictionCol="Prediction",
    maxBins=4657,
    maxMemoryInMB=1024
  )
  model = rfc.fit(final_vectorized_features)

  # Guardar modelo en MinIO
  model_output_path = "{}/spark_random_forest_classifier.flight_delays.5.0.bin".format(
    models_path)
  model.write().overwrite().save(model_output_path)
  print("Modelo guardado en:", model_output_path)

  # ── Evaluar modelo ────────────────────────────────────────────────────────────
  predictions = model.transform(final_vectorized_features)

  from pyspark.ml.evaluation import MulticlassClassificationEvaluator
  evaluator = MulticlassClassificationEvaluator(
    predictionCol="Prediction",
    labelCol="ArrDelayBucket",
    metricName="accuracy"
  )
  accuracy = evaluator.evaluate(predictions)
  print("Accuracy = {}".format(accuracy))

  # ── Registrar en MLflow ───────────────────────────────────────────────────────
  print('Inicicando MLFLOW')
  mlflow.log_param("maxBins", 4657)
  mlflow.log_param("maxMemoryInMB", 1024)
  mlflow.log_param("numTrees", model.getNumTrees)
  mlflow.log_metric("accuracy", accuracy)
  mlflow.end_run()
  print('MLFLOW completado')
  predictions.groupBy("Prediction").count().show()
  predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)
