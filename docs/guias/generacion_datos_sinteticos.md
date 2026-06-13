# Guia: Generacion de Datos Sinteticos (Batch y Streaming)

Esta guia explica como usar el script `scripts/generar_datos_streaming.py` para
crear tus propios datos de practica, en dos modos:

- **Modo batch (archivo):** genera un archivo JSONL o CSV de una vez, ideal para
  practicar ETL, limpieza y Spark SQL (EA1 y EA2).
- **Modo streaming (Kafka):** envia eventos en vivo a un topic de Kafka, ideal
  para Structured Streaming y dashboards en tiempo real (EA3).

> **Requisito:** el entorno Docker debe estar levantado. El modo batch funciona
> con el perfil `basico`; el modo streaming requiere el perfil `completo` (Kafka).

---

## 1. Donde ejecutar el script

El script vive dentro del contenedor de Jupyter en `/home/jovyan/scripts/`.
Tienes dos formas equivalentes de ejecutarlo:

**Opcion A — desde tu terminal (sin entrar al contenedor):**

```bash
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py --help
```

**Opcion B — desde JupyterLab:** abre una Terminal (File > New > Terminal) y ejecuta:

```bash
python /home/jovyan/scripts/generar_datos_streaming.py --help
```

> Los archivos que generes en `/home/jovyan/datos/` apareceran automaticamente
> en la carpeta `datos/` de tu repositorio local (es un volumen compartido).

---

## 2. Tipos de datos disponibles

| Tipo | Descripcion | Batch (archivo) | Streaming (Kafka) |
|------|-------------|:---------------:|:-----------------:|
| `transacciones` | Ventas en tiendas: producto, monto, region, metodo de pago | Si | Si |
| `logs` | Logs de servidor web: endpoint, status code, tiempo de respuesta | Si | Si |
| `iot` | Sensores: temperatura, humedad, presion, bateria | Si | Si |
| `clickstream` | Navegacion web: paginas, clics, sesiones, dispositivo | Si | No |
| `stock` | Ticks bursatiles: precio, cambio %, volumen | Si | No |
| `social` | Posts de redes sociales: hashtags, likes, alcance | Si | No |

---

## 3. Modo BATCH: generar archivos

Genera una cantidad fija de eventos y los escribe a un archivo. Sintaxis general:

```bash
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo <tipo> \
  --archivo /home/jovyan/datos/streaming/<nombre_archivo> \
  --cantidad <n_eventos> \
  --formato <jsonl|csv>
```

### Ejemplos

```bash
# 1000 transacciones en JSONL (formato tipico de Big Data)
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo transacciones --archivo /home/jovyan/datos/streaming/mis_ventas.jsonl --cantidad 1000

# 5000 logs web en CSV
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo logs --archivo /home/jovyan/datos/streaming/mis_logs.csv --cantidad 5000 --formato csv

# 2000 ticks de bolsa con tendencia alcista
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo stock --archivo /home/jovyan/datos/streaming/stock_alcista.jsonl --cantidad 2000 --tendencia alcista
```

### Datos "sucios" para practicar limpieza

Con `--calidad baja` el generador introduce errores intencionales (nulos,
valores imposibles, sensores dañados). Perfecto para los ejercicios de
transformacion y limpieza de EA2:

```bash
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo iot --archivo /home/jovyan/datos/streaming/iot_sucio.jsonl --cantidad 500 --calidad baja
```

### Leer los archivos generados con Spark

En un notebook de JupyterLab:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lectura_batch").getOrCreate()

# JSONL: un JSON por linea
df = spark.read.json("/home/jovyan/datos/streaming/mis_ventas.jsonl")
df.printSchema()
df.show(5)

# CSV con cabecera
df_logs = spark.read.csv("/home/jovyan/datos/streaming/mis_logs.csv", header=True, inferSchema=True)
df_logs.groupBy("status_code").count().show()
```

---

## 4. Modo STREAMING: enviar eventos a Kafka

Envia eventos en vivo a un topic de Kafka a una velocidad controlada.
Solo soporta `transacciones`, `logs` e `iot`. Requiere el perfil `completo`:

```bash
docker compose --profile completo up -d
```

Sintaxis general:

```bash
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo <transacciones|logs|iot> \
  --velocidad <eventos_por_segundo> \
  --duracion <segundos> \
  --topic <nombre_topic>
```

> Si omites `--topic`, se usa `bigdata-<tipo>` (por ejemplo `bigdata-transacciones`).
> El generador se detiene solo al cumplirse la duracion, o con `Ctrl+C`.

### Ejemplos

```bash
# 5 transacciones por segundo durante 10 minutos
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo transacciones --velocidad 5 --duracion 600 --topic transacciones

# 20 logs por segundo durante 2 minutos (simular carga de un servidor)
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo logs --velocidad 20 --duracion 120

# Lecturas IoT durante 5 minutos
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo iot --velocidad 10 --duracion 300
```

### Consumir el stream con Spark Structured Streaming

En un notebook (ver tambien `notebooks/EA3_tiempo_real/03_spark_structured_streaming.ipynb`):

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = (
    SparkSession.builder
    .appName("consumo_kafka")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2")
    .getOrCreate()
)

esquema = StructType([
    StructField("region", StringType()),
    StructField("producto", StringType()),
    StructField("total", LongType()),
])

flujo = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")   # direccion interna del broker
    .option("subscribe", "transacciones")
    .option("startingOffsets", "latest")
    .load()
)

ventas = (
    flujo.selectExpr("CAST(value AS STRING) AS json")
    .select(F.from_json("json", esquema).alias("e"))
    .select("e.*")
    .groupBy("region")
    .agg(F.count("*").alias("n_tx"), F.sum("total").alias("monto"))
)

consulta = ventas.writeStream.outputMode("complete").format("console").start()
# Detener con: consulta.stop()
```

> **Importante:** dentro de los contenedores el broker es `kafka:29092`.
> Desde tu maquina (fuera de Docker) es `localhost:9092`.

---

## 5. Pipeline completo: alimentar el dashboard de Grafana

Para ver el dashboard **"Negocio en vivo"** de Grafana con datos reales, necesitas
dos procesos corriendo en paralelo (cada uno en su propia terminal):

```bash
# Terminal 1: job de Spark que lee de Kafka y escribe agregados en Postgres
docker exec bigdata-jupyter python /home/jovyan/scripts/streaming_a_postgres.py

# Terminal 2: generador de transacciones hacia el topic "transacciones"
docker exec bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo transacciones --velocidad 5 --duracion 3600 --topic transacciones
```

Luego abre Grafana en http://localhost:3000 (o el puerto que tengas en `GRAFANA_PORT`
de tu `.env`) y entra a la carpeta **Big Data > 📊 Negocio en vivo — transacciones**.
En ~10 segundos empezaras a ver el monto acumulado, ventas por region y el
throughput de transacciones actualizandose en vivo.

El flujo completo es:

```
generador (Python) -> Kafka (topic transacciones) -> Spark Structured Streaming -> Postgres (tabla ventas_agg) -> Grafana
```

Este mismo patron es el que en la nube implementarias con
Pub/Sub + Dataflow + BigQuery + Looker (GCP) o Kinesis + KDA + Redshift + QuickSight (AWS).

> **Nota:** si tu entorno ya existia antes de la actualizacion con Grafana, la base
> `analytics` puede no existir. Revisa la seccion "Observabilidad con Grafana" del
> README para crearla manualmente (o ejecuta `scripts/reset_entorno.sh`).

---

## 6. Ideas de simulaciones para practicar

| Escenario | Comandos sugeridos |
|-----------|--------------------|
| **ETL batch con datos sucios** | Genera `iot` con `--calidad baja` y limpia los nulos/anomalias con Spark |
| **Analisis de logs de servidor** | Genera 10.000 `logs` en CSV y calcula tasa de errores 5xx por endpoint |
| **Comparar tendencias bursatiles** | Genera `stock` con `--tendencia alcista` y otro con `bajista`, compara con Spark SQL |
| **Pico de trafico** | Lanza el generador de `logs` con `--velocidad 50` y observa el throughput en el notebook 03 |
| **Dashboard en vivo** | Pipeline completo de la seccion 5 + dashboard de Grafana |
| **Deteccion de anomalias IoT** | Stream de `iot` y filtra temperaturas > 50°C en Structured Streaming |

---

## 7. Resumen de parametros

| Parametro | Modo | Descripcion | Default |
|-----------|------|-------------|---------|
| `--tipo` | ambos | Tipo de datos (obligatorio) | — |
| `--archivo` | batch | Ruta del archivo de salida (activa el modo batch) | — |
| `--cantidad` | batch | Numero de eventos a generar | 1000 |
| `--formato` | batch | `jsonl` o `csv` | jsonl |
| `--velocidad` | streaming | Eventos por segundo | 10 |
| `--duracion` | streaming | Duracion en segundos | 60 |
| `--topic` | streaming | Topic de Kafka destino | `bigdata-<tipo>` |
| `--calidad` | ambos | `alta` o `baja` (errores intencionales) | alta |
| `--tendencia` | ambos | `estable`, `alcista` o `bajista` (solo `stock`) | estable |
