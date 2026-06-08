#!/usr/bin/env python3
"""Job Spark Structured Streaming: Kafka (transacciones) -> agregado -> Postgres.

Alimenta el dashboard "Negocio en vivo" de Grafana. Ejecutar dentro de Jupyter:
    %run /home/jovyan/scripts/streaming_a_postgres.py
o por terminal del contenedor:
    docker exec bigdata-jupyter python /home/jovyan/scripts/streaming_a_postgres.py
Detener con Ctrl+C / interrumpiendo el kernel.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

TOPIC = "transacciones"
PG_URL = "jdbc:postgresql://postgres:5432/analytics"
PG_PROPS = {
    "user": "hive",
    "password": "hive_metastore",
    "driver": "org.postgresql.Driver",
}

spark = (
    SparkSession.builder
    .appName("streaming_a_postgres")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,"
        "org.postgresql:postgresql:42.7.4",
    )
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Solo declaramos los campos que usamos; from_json ignora el resto del evento.
esquema = StructType([
    StructField("id", StringType()),
    StructField("region", StringType()),
    StructField("producto", StringType()),
    StructField("total", LongType()),
    StructField("cantidad", LongType()),
])

flujo = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)
eventos = (
    flujo.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", esquema).alias("e"))
    .select("e.*")
    .filter(F.col("region").isNotNull())
)


def escribir_batch(batch_df, batch_id):
    agg = (
        batch_df.groupBy("region")
        .agg(F.count("*").alias("n_tx"), F.sum("total").alias("monto_total"))
        .withColumn("actualizado_en", F.current_timestamp())
    )
    agg.write.mode("append").jdbc(PG_URL, "ventas_agg", properties=PG_PROPS)
    print(f"[batch {batch_id}] regiones escritas: {agg.count()}")


consulta = (
    eventos.writeStream
    .foreachBatch(escribir_batch)
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)
print(f"Streaming Kafka -> Postgres iniciado. Topic: {TOPIC}")
consulta.awaitTermination()
