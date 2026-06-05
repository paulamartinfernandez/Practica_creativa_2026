package es.upm.dit.ging.predictor

import com.mongodb.spark._
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.functions.{concat, from_json, lit, to_json, struct}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Flight predictor starting...")

    val spark = SparkSession
      .builder
      .appName("FlightDelayPredictor")
      .config("spark.cassandra.connection.host",
        sys.env.getOrElse("CASSANDRA_HOST", "cassandra"))
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("MINIO_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("MINIO_SECRET_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()
    import spark.implicits._

    // ── Rutas del modelo ──────────────────────────────────────────────────────
    val modelsBucket = sys.env.getOrElse("MINIO_BUCKET_MODELS", "models")
    
    val base_path = s"s3a://$modelsBucket"
    val arrivalBucketizerPath = s"$base_path/arrival_bucketizer_2.0.bin"
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)

    val columns = Seq("Carrier", "Origin", "Dest", "Route")
    val stringIndexerModelPath = columns.map(n =>
      s"$base_path/string_indexer_model_$n.bin")
    val stringIndexerModel = stringIndexerModelPath.map(n =>
      StringIndexerModel.load(n))
    val stringIndexerModels = (columns zip stringIndexerModel).toMap

    val vectorAssemblerPath = s"$base_path/numeric_vector_assembler.bin"
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    val randomForestModelPath =
      s"$base_path/spark_random_forest_classifier.flight_delays.5.0.bin"
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    // ── Leer de Kafka ─────────────────────────────────────────────────────────
    val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "kafka:9092")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "flight-delay-ml-request")
      .load()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct_schema = new StructType()
      .add("Origin",       DataTypes.StringType)
      .add("FlightNum",    DataTypes.StringType)
      .add("DayOfWeek",    DataTypes.IntegerType)
      .add("DayOfYear",    DataTypes.IntegerType)
      .add("DayOfMonth",   DataTypes.IntegerType)
      .add("Dest",         DataTypes.StringType)
      .add("DepDelay",     DataTypes.DoubleType)
      .add("Prediction",   DataTypes.StringType)
      .add("Timestamp",    DataTypes.TimestampType)
      .add("FlightDate",   DataTypes.DateType)
      .add("Carrier",      DataTypes.StringType)
      .add("UUID",         DataTypes.StringType)
      .add("Distance",     DataTypes.DoubleType)
      .add("Carrier_index",DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index",   DataTypes.DoubleType)
      .add("Route_index",  DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(
      from_json($"value", struct_schema).as("flight"))

    val flightFlattenedDf = flightNestedDf.selectExpr(
      "flight.Origin", "flight.DayOfWeek", "flight.DayOfYear",
      "flight.DayOfMonth", "flight.Dest", "flight.DepDelay",
      "flight.Timestamp", "flight.FlightDate", "flight.Carrier",
      "flight.UUID", "flight.Distance")

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route", concat(flightFlattenedDf("Origin"), lit('-'), flightFlattenedDf("Dest")))

    val flightFlattenedDf2 = flightNestedDf.selectExpr(
      "flight.Origin", "flight.DayOfWeek", "flight.DayOfYear",
      "flight.DayOfMonth", "flight.Dest", "flight.DepDelay",
      "flight.Timestamp", "flight.FlightDate", "flight.Carrier",
      "flight.UUID", "flight.Distance",
      "flight.Carrier_index", "flight.Origin_index",
      "flight.Dest_index", "flight.Route_index")

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route", concat(flightFlattenedDf2("Origin"), lit('-'), flightFlattenedDf2("Dest")))

    val predictionRequestsWithRoute = stringIndexerModel.map(
      n => n.transform(predictionRequestsWithRouteMod))

    val vectorizedFeatures = vectorAssembler
      .setHandleInvalid("keep")
      .transform(predictionRequestsWithRouteMod2)

    val finalVectorizedFeatures = vectorizedFeatures
      .drop("Carrier_index").drop("Origin_index")
      .drop("Dest_index").drop("Route_index")

    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    val finalPredictions = predictions
      .drop("indices").drop("values")
      .drop("rawPrediction").drop("probability")

    // ── 1. Escribir en MongoDB (comportamiento original) ──────────────────────
    val mongoQuery = finalPredictions
      .writeStream
      .format("mongodb")
      .option("spark.mongodb.connection.uri",
        sys.env.getOrElse("MONGO_URI", "mongodb://mongo:27017"))
      .option("spark.mongodb.database", "agile_data_science")
      .option("spark.mongodb.collection", "flight_delay_ml_response")
      .option("checkpointLocation", "s3a://models/checkpoints/mongo")
      .outputMode("append")
      .start()

    // ── 2. Escribir en Cassandra ──────────────────────────────────────────────
    val cassandraQuery = finalPredictions
      .writeStream
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        batchDf
          .select("UUID", "Origin", "Dest", "Carrier", "FlightDate",
                  "DepDelay", "Distance", "prediction", "Timestamp")
          .withColumnRenamed("UUID", "uuid")
          .withColumnRenamed("Origin", "origin")
          .withColumnRenamed("Dest", "dest")
          .withColumnRenamed("Carrier", "carrier")
          .withColumnRenamed("FlightDate", "flight_date")
          .withColumnRenamed("DepDelay", "dep_delay")
          .withColumnRenamed("Distance", "distance")
          .withColumnRenamed("Timestamp", "timestamp")
          .write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "agile_data_science")
          .option("table", "flight_delay_ml_response")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "s3a://models/checkpoints/cassandra")
      .outputMode("append")
      .start()

    // ── 3. Escribir resultado en Kafka (topic de respuesta) ───────────────────
    val kafkaResponseQuery = finalPredictions
      .selectExpr(
        "UUID AS key",
        "to_json(struct(UUID, Origin, Dest, Carrier, FlightDate, DepDelay, Distance, prediction, Timestamp)) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "s3a://models/checkpoints/kafka_response")
      .outputMode("append")
      .start()

    // ── Consola para debug ────────────────────────────────────────────────────
    val consoleOutput = finalPredictions
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    consoleOutput.awaitTermination()
  }
}
