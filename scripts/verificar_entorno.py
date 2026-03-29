#!/usr/bin/env python3
"""
Script de verificacion del entorno Big Data.
Ejecutar dentro de JupyterLab para confirmar que todo funciona.

Uso:
    %run /home/jovyan/scripts/verificar_entorno.py
"""

import sys
import os


def check(nombre, funcion):
    """Ejecuta una verificacion y muestra el resultado."""
    try:
        resultado = funcion()
        if resultado:
            print(f"  [PASS] {nombre}")
            return True
        else:
            print(f"  [FAIL] {nombre}")
            return False
    except Exception as e:
        print(f"  [FAIL] {nombre} -> {e}")
        return False


def verificar_python():
    """Verifica version de Python."""
    version = sys.version_info
    return version.major == 3 and version.minor >= 10


def verificar_pyspark():
    """Verifica que PySpark esta instalado y puede crear SparkSession."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("verificacion_entorno") \
        .master("local[*]") \
        .getOrCreate()
    version = spark.version
    spark.stop()
    return version.startswith("3.5")


def verificar_librerias():
    """Verifica librerias Python necesarias."""
    librerias = ["pandas", "numpy", "matplotlib", "seaborn", "openpyxl", "plotly"]
    faltantes = []
    for lib in librerias:
        try:
            __import__(lib)
        except ImportError:
            faltantes.append(lib)
    if faltantes:
        print(f"    Faltantes: {', '.join(faltantes)}")
        return False
    return True


def verificar_datasets():
    """Verifica que los datasets estan accesibles."""
    datos_dir = "/home/jovyan/datos"
    archivos = [
        "flights.csv", "sales.csv", "stores.csv",
        "netflix_titles.csv", "ufo_sightings.xlsx", "cartas_magic.csv"
    ]
    faltantes = [a for a in archivos if not os.path.exists(os.path.join(datos_dir, a))]
    if faltantes:
        print(f"    Faltantes: {', '.join(faltantes)}")
        return False
    return True


def verificar_spark_csv():
    """Verifica que Spark puede leer un CSV."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("verificacion_csv") \
        .master("local[*]") \
        .getOrCreate()
    df = spark.read.csv("/home/jovyan/datos/flights.csv", header=True, inferSchema=True, nullValue="NA")
    count = df.count()
    spark.stop()
    return count > 0


def verificar_spark_operaciones():
    """Verifica operaciones basicas de Spark."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    spark = SparkSession.builder \
        .appName("verificacion_ops") \
        .master("local[*]") \
        .getOrCreate()
    df = spark.read.csv("/home/jovyan/datos/stores.csv", header=True, inferSchema=True)
    resultado = df.groupBy("Type").agg(F.count("*").alias("total"))
    count = resultado.count()
    spark.stop()
    return count > 0


def verificar_parquet():
    """Verifica escritura y lectura de Parquet."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("verificacion_parquet") \
        .master("local[*]") \
        .getOrCreate()
    parquet_path = "/tmp/verificacion_parquet"
    df = spark.createDataFrame([(1, "test"), (2, "ok")], ["id", "valor"])
    df.write.mode("overwrite").parquet(parquet_path)
    df_leido = spark.read.parquet(parquet_path)
    count = df_leido.count()
    spark.stop()
    return count == 2


def verificar_kafka():
    """Verifica conexion a Kafka."""
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        request_timeout_ms=5000,
        max_block_ms=5000
    )
    producer.close()
    return True


def verificar_hive():
    """Verifica conexion a Hive Metastore."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("verificacion_hive") \
        .master("local[*]") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("SHOW DATABASES").show()
    spark.stop()
    return True


def main():
    print("=" * 55)
    print("  VERIFICACION DEL ENTORNO BIG DATA")
    print("  Curso BIY7131 - DUOC 2026-1")
    print("=" * 55)

    total = 0
    pasados = 0

    # --- Verificaciones basicas (perfil basico) ---
    print("\n--- Verificaciones basicas ---")
    basicas = [
        ("Python >= 3.10", verificar_python),
        ("PySpark 3.5.x instalado", verificar_pyspark),
        ("Librerias Python (pandas, numpy, matplotlib, etc.)", verificar_librerias),
        ("Datasets accesibles", verificar_datasets),
        ("Spark puede leer CSV", verificar_spark_csv),
        ("Spark groupBy/agg funciona", verificar_spark_operaciones),
        ("Spark escribe/lee Parquet", verificar_parquet),
    ]

    for nombre, func in basicas:
        total += 1
        if check(nombre, func):
            pasados += 1

    # --- Verificaciones perfil completo ---
    print("\n--- Verificaciones perfil completo (opcional) ---")
    avanzadas = [
        ("Kafka accesible", verificar_kafka),
        ("Hive Metastore accesible", verificar_hive),
    ]

    for nombre, func in avanzadas:
        total += 1
        if check(nombre, func):
            pasados += 1

    # --- Resumen ---
    print("\n" + "=" * 55)
    print(f"  RESULTADO: {pasados}/{total} verificaciones pasaron")
    if pasados >= 7:
        print("  Entorno basico OK - listo para EA1 y EA2")
    if pasados == total:
        print("  Entorno completo OK - listo para EA3")
    elif pasados < 7:
        print("  ATENCION: hay problemas en el entorno basico")
        print("  Revisa la GUIA_INSTALACION.md para soluciones")
    print("=" * 55)


if __name__ == "__main__":
    main()
