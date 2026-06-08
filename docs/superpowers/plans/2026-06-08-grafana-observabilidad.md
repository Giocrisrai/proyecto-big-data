# Observabilidad con Grafana — Plan de Implementación

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Agregar un stack de observabilidad (Grafana + Prometheus + cAdvisor + kafka-exporter) al perfil `completo`, con dashboards pre-provisionados de infraestructura y de negocio en vivo, cada panel anotado con su equivalente cloud, + un notebook con mini-ejercicio.

**Architecture:** Prometheus scrapea cAdvisor (métricas de contenedores) y kafka-exporter (lag/throughput de Kafka) → Grafana los grafica (dashboard Infra). Un job Spark Structured Streaming consume Kafka, agrega y escribe a Postgres (DB `analytics`) → Grafana lo grafica (dashboard Negocio). Todo provisionado como código.

**Tech Stack:** Docker Compose, Grafana OSS, Prometheus, cAdvisor, kafka-exporter, PySpark 4.1 Structured Streaming, PostgreSQL 16.

**Validación en lugar de tests unitarios:** este feature es infra/config; los "tests" son comandos de verificación (curl a APIs, `docker ps`, queries) con salida esperada. Backend Docker: en cada comando exportar `DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"` (Colima; ver [[docker-backend-colima]]).

---

## Estructura de archivos

```
docker-compose.yml                                       (modificar: +4 servicios, +2 volúmenes)
docker/prometheus/prometheus.yml                         (crear)
docker/grafana/provisioning/datasources/datasources.yml  (crear)
docker/grafana/provisioning/dashboards/dashboards.yml    (crear: provider)
docker/grafana/provisioning/dashboards/infra.json        (crear)
docker/grafana/provisioning/dashboards/negocio.json      (crear)
docker/postgres/initdb/01_analytics.sql                  (crear)
scripts/streaming_a_postgres.py                          (crear)
notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb (crear)
README.md                                                (modificar: puertos + símil + uso)
```

---

## Task 1: Prometheus + exporters en el compose

**Files:**
- Create: `docker/prometheus/prometheus.yml`
- Modify: `docker-compose.yml` (añadir servicios prometheus, cadvisor, kafka-exporter; añadir volumen prometheus-data)

- [ ] **Step 1: Crear `docker/prometheus/prometheus.yml`**

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs:
      - targets: ["localhost:9090"]

  - job_name: cadvisor
    static_configs:
      - targets: ["cadvisor:8080"]

  - job_name: kafka
    static_configs:
      - targets: ["kafka-exporter:9308"]
```

- [ ] **Step 2: Añadir servicios al `docker-compose.yml`** (después del bloque `hive-server`, antes de `networks:`)

```yaml
  # ============================================
  # OBSERVABILIDAD — Grafana + Prometheus (perfil completo)
  # ============================================

  prometheus:
    image: prom/prometheus:v3.1.0
    container_name: bigdata-prometheus
    profiles: ["completo"]
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    networks:
      - bigdata-net
    restart: unless-stopped

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    container_name: bigdata-cadvisor
    profiles: ["completo"]
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
    networks:
      - bigdata-net
    restart: unless-stopped

  kafka-exporter:
    image: danielqsj/kafka-exporter:v1.8.0
    container_name: bigdata-kafka-exporter
    profiles: ["completo"]
    command: ["--kafka.server=kafka:29092"]
    networks:
      - bigdata-net
    depends_on:
      - kafka
    restart: unless-stopped
```

- [ ] **Step 3: Añadir volumen** (en la sección `volumes:` al final)

```yaml
  prometheus-data:
```

- [ ] **Step 4: Levantar y validar targets de Prometheus**

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker compose --profile completo up -d prometheus cadvisor kafka-exporter
sleep 30
curl -s "http://localhost:9090/api/v1/targets" | python3 -c "import sys,json; d=json.load(sys.stdin); [print(t['labels']['job'], t['health']) for t in d['data']['activeTargets']]"
```
Expected: `cadvisor up`, `kafka up`, `prometheus up`. Si cadvisor sale `down`, ver Task 1b.

- [ ] **Step 5: Commit**

```bash
git add docker/prometheus/prometheus.yml docker-compose.yml
git commit -m "feat(obs): prometheus + cadvisor + kafka-exporter en perfil completo"
```

## Task 1b: Fallback cAdvisor (solo si Step 4 lo mostró `down`)

- [ ] **Step 1:** Revisar logs: `docker logs bigdata-cadvisor 2>&1 | tail -20`.
- [ ] **Step 2:** Si falla por mounts en Colima, probar quitar `/var/lib/docker` del mount y añadir `devices: ["/dev/kmsg"]`. Si sigue fallando, documentar en README que las métricas de contenedor pueden requerir `node-exporter`; el dashboard de negocio + métricas de Kafka funcionan igual. No bloquear el resto del plan.

---

## Task 2: Grafana + provisioning de datasources

**Files:**
- Create: `docker/grafana/provisioning/datasources/datasources.yml`
- Create: `docker/grafana/provisioning/dashboards/dashboards.yml`
- Modify: `docker-compose.yml` (servicio grafana + volumen grafana-data)

- [ ] **Step 1: Crear `docker/grafana/provisioning/datasources/datasources.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
  - name: Postgres-Analytics
    type: postgres
    access: proxy
    url: postgres:5432
    database: analytics
    user: hive
    secureJsonData:
      password: hive_metastore
    jsonData:
      sslmode: disable
      postgresVersion: 1600
```

- [ ] **Step 2: Crear `docker/grafana/provisioning/dashboards/dashboards.yml`** (provider)

```yaml
apiVersion: 1
providers:
  - name: bigdata
    folder: "Big Data"
    type: file
    disableDeletion: false
    editable: true
    options:
      path: /etc/grafana/provisioning/dashboards
      foldersFromFilesStructure: false
```

- [ ] **Step 3: Añadir servicio grafana al `docker-compose.yml`** (en el bloque de observabilidad)

```yaml
  grafana:
    image: grafana/grafana-oss:11.4.0
    container_name: bigdata-grafana
    profiles: ["completo"]
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_USER=admin
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_PASSWORD:-bigdata2026}
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
      - GF_USERS_DEFAULT_THEME=light
    volumes:
      - ./docker/grafana/provisioning:/etc/grafana/provisioning:ro
      - grafana-data:/var/lib/grafana
    networks:
      - bigdata-net
    depends_on:
      - prometheus
    restart: unless-stopped
```

- [ ] **Step 4: Añadir volumen** `grafana-data:` en la sección `volumes:`.

- [ ] **Step 5: Levantar y validar Grafana + datasources**

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker compose --profile completo up -d grafana
sleep 20
curl -s -u admin:bigdata2026 "http://localhost:3000/api/datasources" | python3 -c "import sys,json;[print(d['name'],d['type']) for d in json.load(sys.stdin)]"
```
Expected: `Prometheus prometheus` y `Postgres-Analytics grafana-postgresql-datasource` (o `postgres`).

- [ ] **Step 6: Commit**

```bash
git add docker/grafana docker-compose.yml
git commit -m "feat(obs): grafana con datasources provisionados (prometheus + postgres)"
```

---

## Task 3: Dashboard de Infraestructura

**Files:**
- Create: `docker/grafana/provisioning/dashboards/infra.json`

- [ ] **Step 1: Crear `infra.json`** — dashboard con datasource Prometheus. Paneles (todos `gridPos` en cuadrícula de 24):
  - **Panel "Servicios arriba/abajo"** (type `stat`): query `up`, legend `{{job}}`.
  - **Panel "CPU por contenedor"** (type `timeseries`): query
    `rate(container_cpu_usage_seconds_total{name=~"bigdata-.*"}[1m])`, legend `{{name}}`.
  - **Panel "Memoria por contenedor"** (type `timeseries`): query
    `container_memory_usage_bytes{name=~"bigdata-.*"}`, legend `{{name}}`, unit `bytes`.
  - **Panel "Kafka — mensajes por topic"** (type `timeseries`): query
    `sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*"}[1m]))`.
  - **Panel "Kafka — lag de consumidores"** (type `timeseries`): query
    `sum by (consumergroup, topic) (kafka_consumergroup_lag)`.
  - **Panel de texto "Símil cloud"** (type `text`, markdown): tabla local→GCP/AWS/Azure
    (Grafana=Cloud Monitoring/CloudWatch/Azure Monitor; Prometheus=Managed Prometheus;
    cAdvisor=métricas de contenedor GKE/EKS/AKS; kafka-exporter=métricas Pub/Sub/Kinesis/Event Hubs).

  El JSON debe envolver los paneles con: `"schemaVersion": 39`, `"title": "🩺 Infraestructura — Big Data local"`, `"uid": "bigdata-infra"`, `"time": {"from":"now-15m","to":"now"}`, `"refresh": "10s"`, y cada panel con su `datasource` Prometheus (`{"type":"prometheus","uid":"${DS_PROMETHEUS}"}` o por nombre). Usar datasource por nombre `"Prometheus"` para que matchee el provisionado.

- [ ] **Step 2: Recargar Grafana y validar que el dashboard carga**

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker restart bigdata-grafana && sleep 15
curl -s -u admin:bigdata2026 "http://localhost:3000/api/search?query=Infraestructura" | python3 -c "import sys,json; print([d['title'] for d in json.load(sys.stdin)])"
```
Expected: incluye `🩺 Infraestructura — Big Data local`.

- [ ] **Step 3: Validar que un panel devuelve datos** (query a Prometheus directo)

```bash
curl -s "http://localhost:9090/api/v1/query?query=container_memory_usage_bytes%7Bname%3D~%22bigdata-.%2A%22%7D" | python3 -c "import sys,json; d=json.load(sys.stdin); print('series:', len(d['data']['result']))"
```
Expected: `series:` > 0 (si cadvisor está up). Si cadvisor falló (Task 1b), validar al menos la métrica `up`.

- [ ] **Step 4: Commit**

```bash
git add docker/grafana/provisioning/dashboards/infra.json
git commit -m "feat(obs): dashboard de infraestructura con símil cloud"
```

---

## Task 4: DB analytics + job Spark streaming → Postgres

**Files:**
- Create: `docker/postgres/initdb/01_analytics.sql`
- Create: `scripts/streaming_a_postgres.py`
- Modify: `docker-compose.yml` (montar initdb en postgres)

- [ ] **Step 1: Crear `docker/postgres/initdb/01_analytics.sql`**

```sql
-- Se ejecuta SOLO en un volumen de datos limpio (primer arranque de Postgres).
-- Crea la base 'analytics' y la tabla de agregados que alimenta Grafana.
CREATE DATABASE analytics;
\connect analytics
CREATE TABLE IF NOT EXISTS ventas_agg (
    region          TEXT,
    n_tx            BIGINT,
    monto_total     BIGINT,
    actualizado_en  TIMESTAMP DEFAULT now()
);
```

- [ ] **Step 2: Montar initdb en el servicio `postgres` del compose** (añadir a su `volumes:`)

```yaml
      - ./docker/postgres/initdb:/docker-entrypoint-initdb.d:ro
```

- [ ] **Step 3: Crear la DB analytics también para entornos YA instalados** (idempotente; el initdb no corre si el volumen ya existe)

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker exec bigdata-postgres psql -U hive -d metastore -tc "SELECT 1 FROM pg_database WHERE datname='analytics'" | grep -q 1 || docker exec bigdata-postgres psql -U hive -d metastore -c "CREATE DATABASE analytics"
docker exec bigdata-postgres psql -U hive -d analytics -c "CREATE TABLE IF NOT EXISTS ventas_agg (region TEXT, n_tx BIGINT, monto_total BIGINT, actualizado_en TIMESTAMP DEFAULT now());"
```
Expected: `CREATE TABLE` (o sin error si ya existe).

- [ ] **Step 4: Crear `scripts/streaming_a_postgres.py`**

```python
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
    (agg.write.mode("append").jdbc(PG_URL, "ventas_agg", properties=PG_PROPS))
    print(f"[batch {batch_id}] filas escritas: {agg.count()}")


consulta = (
    eventos.writeStream
    .foreachBatch(escribir_batch)
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start()
)
print("Streaming Kafka -> Postgres iniciado. Topic:", TOPIC)
consulta.awaitTermination()
```

  NOTA: el evento del generador `transacciones` trae más campos (monto, tienda_id,
  etc.), pero el esquema solo declara los que usamos; `from_json` ignora el resto.

- [ ] **Step 5: Validar el pipeline end-to-end** (producir eventos + correr el job unos segundos + leer Postgres)

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
# 1) producir transacciones en background
docker exec -d bigdata-jupyter bash -lc 'python /home/jovyan/scripts/generar_datos_streaming.py --tipo transacciones --velocidad 10 --duracion 90 --topic transacciones'
# 2) correr el job de streaming ~70s y cortarlo
docker exec bigdata-jupyter bash -lc 'cd /home/jovyan && timeout 70 python /home/jovyan/scripts/streaming_a_postgres.py' 2>&1 | tail -5
# 3) verificar filas en Postgres
docker exec bigdata-postgres psql -U hive -d analytics -c "SELECT region, count(*) lotes, sum(n_tx) tx, sum(monto_total) monto FROM ventas_agg GROUP BY region;"
```
Expected: filas por región con `tx` y `monto` > 0.

- [ ] **Step 6: Commit**

```bash
git add docker/postgres/initdb/01_analytics.sql scripts/streaming_a_postgres.py docker-compose.yml
git commit -m "feat(obs): DB analytics + job Spark streaming Kafka->Postgres"
```

---

## Task 5: Dashboard de Negocio en vivo

**Files:**
- Create: `docker/grafana/provisioning/dashboards/negocio.json`

- [ ] **Step 1: Crear `negocio.json`** — datasource `Postgres-Analytics`. Paneles:
  - **"Monto por región"** (type `barchart`): SQL
    `SELECT region, SUM(monto_total) AS monto FROM ventas_agg GROUP BY region ORDER BY monto DESC;`
  - **"Transacciones por región (tiempo)"** (type `timeseries`): SQL con macro de tiempo de Grafana
    `SELECT actualizado_en AS time, region AS metric, n_tx FROM ventas_agg WHERE $__timeFilter(actualizado_en) ORDER BY actualizado_en;`
  - **"Monto total acumulado"** (type `stat`): SQL
    `SELECT SUM(monto_total) AS monto_total FROM ventas_agg;`
  - **"Últimos lotes procesados"** (type `table`): SQL
    `SELECT actualizado_en, region, n_tx, monto_total FROM ventas_agg ORDER BY actualizado_en DESC LIMIT 20;`
  - **Panel de texto "Símil cloud"**: Postgres(serving)=BigQuery/Redshift/Synapse;
    Spark Streaming=Dataflow/Kinesis Analytics/Stream Analytics; Grafana=Looker Studio/QuickSight/Power BI.

  Envoltura JSON: `"title": "📊 Negocio en vivo — transacciones"`, `"uid": "bigdata-negocio"`,
  `"refresh": "5s"`, `"time": {"from":"now-15m","to":"now"}`. Cada panel con
  `"datasource": {"type":"postgres","uid":"..."}` o por nombre `"Postgres-Analytics"`.

- [ ] **Step 2: Recargar y validar que carga**

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker restart bigdata-grafana && sleep 15
curl -s -u admin:bigdata2026 "http://localhost:3000/api/search?query=Negocio" | python3 -c "import sys,json; print([d['title'] for d in json.load(sys.stdin)])"
```
Expected: incluye `📊 Negocio en vivo — transacciones`.

- [ ] **Step 3: Validar datos vía la API de query de Grafana (datasource Postgres)** — alternativamente, confiar en Task 4 Step 5 (datos ya en Postgres) + abrir el panel. Verificar conteo:

```bash
docker exec bigdata-postgres psql -U hive -d analytics -tc "SELECT count(*) FROM ventas_agg;"
```
Expected: > 0.

- [ ] **Step 4: Commit**

```bash
git add docker/grafana/provisioning/dashboards/negocio.json
git commit -m "feat(obs): dashboard de negocio en vivo (Postgres) con símil cloud"
```

---

## Task 6: Notebook con mini-ejercicio

**Files:**
- Create: `notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb`

- [ ] **Step 1: Crear el notebook** (vía un script generador temporal `scripts/_build_nb07.py` con nbformat, ejecutado en el contenedor, y borrado después). Celdas:
  - **MD portada:** qué es observabilidad, el stack (Grafana/Prometheus/cAdvisor/kafka-exporter), requisito perfil completo, tabla símil cloud.
  - **MD "Cómo ver los dashboards":** abrir `http://localhost:3000` (acceso anónimo), carpeta "Big Data", dashboards Infra y Negocio.
  - **CODE:** arrancar el generador de transacciones (subprocess en background) — `import subprocess; subprocess.Popen([...generar_datos_streaming.py --tipo transacciones --velocidad 8 --duracion 300 --topic transacciones])` y mensaje.
  - **MD:** "ahora corré el job de streaming" con el comando `%run /home/jovyan/scripts/streaming_a_postgres.py` (explicando que queda corriendo; interrumpir el kernel para frenar).
  - **MD Ejercicio 1:** agregar al dashboard de Negocio un panel "Top 5 productos por monto". Da la pista de query base y que `producto` no está agregado aún → sugiere modificar el job para agrupar también por producto, o crear tabla nueva. (Ejercicio conceptual + SQL.)
  - **MD Ejercicio 2:** agregar al dashboard de Infra un panel con la memoria del contenedor `bigdata-kafka` usando la métrica `container_memory_usage_bytes{name="bigdata-kafka"}`.
  - **MD cierre:** símil cloud y "quién opera qué".

- [ ] **Step 2: Validar nbformat** del notebook generado:

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker exec bigdata-jupyter python3 -c "import nbformat; nbformat.read(open('/home/jovyan/notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb'),4); print('nbformat OK')"
```
Expected: `nbformat OK`. (No se ejecuta entero porque el job de streaming es de larga duración; las celdas son demostrativas/guía.)

- [ ] **Step 3: Borrar el generador temporal y commitear**

```bash
rm -f scripts/_build_nb07.py
git add notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb
git commit -m "feat(obs): notebook 07 observabilidad con mini-ejercicio guiado"
```

---

## Task 7: README + validación final end-to-end

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Actualizar README** — añadir a la tabla "Puertos y URLs": Grafana 3000 (`http://localhost:3000`, anónimo lectura), Prometheus 9090. Añadir badge/sección "Observabilidad" describiendo el stack y el símil cloud. Añadir `07_observabilidad_grafana.ipynb` a la tabla de EA3. Nota de recursos: si va justo, `colima stop && colima start --cpu 4 --memory 10`. Nota: entornos ya instalados deben crear la DB `analytics` (Task 4 Step 3) o recrear el volumen de postgres.

- [ ] **Step 2: Validación final completa** — levantar todo limpio y verificar los criterios de aceptación del spec:

```bash
export DOCKER_HOST="unix:///Users/giocrisraigodoy/.colima/default/docker.sock"
docker compose --profile completo up -d
sleep 40
echo "=== contenedores (esperado: 10 Up) ==="; docker ps --format '{{.Names}} {{.Status}}' | grep bigdata | wc -l
echo "=== targets prometheus ==="; curl -s "http://localhost:9090/api/v1/targets" | python3 -c "import sys,json;[print(t['labels']['job'],t['health']) for t in json.load(sys.stdin)['data']['activeTargets']]"
echo "=== dashboards grafana ==="; curl -s -u admin:bigdata2026 "http://localhost:3000/api/search?type=dash-db" | python3 -c "import sys,json;[print(d['title']) for d in json.load(sys.stdin)]"
```
Expected: 10 contenedores Up; targets cadvisor/kafka/prometheus `up`; 2 dashboards listados.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(obs): README con puertos de Grafana/Prometheus y símil cloud"
```

- [ ] **Step 4: Push**

```bash
git push origin main
```

---

## Notas de ejecución

- Imágenes pineadas (verificar arm64 al ejecutar; ajustar tag si alguna no tiene arm64): `prom/prometheus:v3.1.0`, `gcr.io/cadvisor/cadvisor:v0.49.1`, `danielqsj/kafka-exporter:v1.8.0`, `grafana/grafana-oss:11.4.0`.
- El job de streaming es de larga duración: en clase se corre y se deja, se frena interrumpiendo el kernel/`Ctrl+C`.
- Riesgo principal: cAdvisor en Colima (Task 1b cubre el fallback). No bloquea negocio ni Kafka.
- Validar SIEMPRE con el backend Colima (`DOCKER_HOST`), no OrbStack. Ver [[docker-backend-colima]].
```
