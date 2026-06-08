# Diseño: Observabilidad con Grafana (servicios locales vs nube)

- **Fecha:** 2026-06-08
- **Curso:** Big Data BIY7131 — DUOC 2026-1
- **Autor:** Giocrisrai Godoy (con Claude Code)
- **Estado:** Aprobado para implementación

## 1. Objetivo

Agregar un stack de **observabilidad** al entorno Docker del curso para que los
alumnos **vean los servicios funcionando en tiempo real** en Grafana, y entiendan
que es otro **servicio que se opera en local** con un **equivalente gestionado en
la nube**. Refuerza la narrativa "local vs nube" del proyecto y conecta toda la
cadena de streaming (serving layer de Lambda/Kappa).

Decisiones tomadas en el brainstorming:
- **Alcance:** dashboards de **infraestructura** + **datos del negocio** (ambos).
- **Modo:** dashboards **pre-provisionados** (funcionan al levantar) **+ un
  mini-ejercicio guiado**.
- **Recursos:** integrado al **perfil `completo`** (un solo comando).
- **Enfoque:** **A (completo)** — Grafana + Prometheus + cAdvisor + kafka-exporter.

## 2. Arquitectura

Se agregan 4 contenedores al perfil `completo`:

| Contenedor | Imagen (sugerida) | Rol | Puerto host |
|---|---|---|---|
| `grafana` | `grafana/grafana-oss` | Dashboards (cara visible) | 3000 |
| `prometheus` | `prom/prometheus` | TSDB + scraping de métricas técnicas | 9090 |
| `cadvisor` | `gcr.io/cadvisor/cadvisor` | Métricas CPU/RAM/red por contenedor | interno |
| `kafka-exporter` | `danielqsj/kafka-exporter` | Lag de consumidores + mensajes/seg de Kafka | interno |

Las versiones exactas de imágenes se fijan en la fase de implementación tras
validar compatibilidad (no usar `latest` flotante en el compose final).

### Dos flujos de datos

**Infraestructura (métricas técnicas):**
```
cadvisor ─┐
          ├─► Prometheus ─► Grafana (dashboard Infraestructura)
kafka-exporter ─┘
```

**Negocio (datos en vivo):**
```
generar_datos_streaming.py ─► Kafka ─► Spark Structured Streaming
   ─► Postgres (tabla agregada) ─► Grafana (dashboard Negocio en vivo)
```

El flujo de negocio reutiliza el Postgres existente (el del Hive Metastore) y
materializa el *serving layer* de las arquitecturas Lambda/Kappa de EA1.

## 3. Componentes — detalle

### 3.1 Grafana
- Provisioning como código (no configuración manual):
  - **Datasources** (auto): Prometheus (`http://prometheus:9090`) y Postgres
    (DB `analytics`).
  - **Dashboards** (auto): se cargan desde JSON montados.
- **Acceso anónimo de solo lectura** habilitado (los alumnos abren
  `http://localhost:3000` sin login). Usuario admin disponible para editar
  (admin/admin con cambio recomendado; aceptable para entorno educativo local).

### 3.2 Prometheus
- `prometheus.yml` con scrape jobs: `cadvisor`, `kafka-exporter`, `prometheus`
  (self). Intervalo ~15s.
- Puerto 9090 expuesto para enseñar PromQL (opcional para el alumno).

### 3.3 cAdvisor
- Métricas de todos los contenedores (CPU, memoria, red, FS).
- Requiere montar (solo lectura): el socket/recursos de Docker y `/sys`,
  `/var/lib/docker`. **Riesgo a validar:** funcionamiento bajo Colima (VM Lima).
  Si los mounts no funcionan en Colima, fallback documentado: usar
  `node-exporter` + métricas del host, o degradar a métricas básicas. Se
  decide tras probar en la implementación.
- No se expone puerto al host (Prometheus lo scrapea por red interna).

### 3.4 kafka-exporter
- Se conecta a `kafka:29092` y expone en `:9308`.
- Da las métricas "lindas" de Kafka: offsets por topic, **lag por grupo de
  consumidores**, mensajes/seg. Mapea directo a métricas de Pub/Sub/Kinesis.
- No se expone al host.

### 3.5 Pipeline de negocio (Spark → Postgres)
- **Base de datos `analytics`** en el Postgres existente, creada vía
  `docker/postgres/initdb/01_analytics.sql` (se ejecuta solo en volumen
  limpio). Para entornos ya instalados, el script de streaming hace
  `CREATE TABLE IF NOT EXISTS` y se documenta el caso (o `down -v` una vez).
- `scripts/streaming_a_postgres.py`: job Spark Structured Streaming que
  consume el topic de transacciones, agrega por región (y/o ventana de tiempo)
  y escribe a Postgres vía JDBC (`org.postgresql:postgresql:42.7.4` por
  `spark.jars.packages`). Usa `foreachBatch` para el upsert/append a Postgres.
- Tabla destino (ejemplo): `analytics.ventas_agg(region, ventana, n_tx,
  monto_total, actualizado_en)`.

### 3.6 Dashboards (provisionados)
1. **Infraestructura** (`infra.json`):
   - CPU % y memoria por contenedor (cadvisor).
   - Estado up/down de cada servicio (`up` de Prometheus).
   - Kafka: mensajes/seg por topic y **lag por grupo** (kafka-exporter).
   - Panel de texto con el símil cloud.
2. **Negocio en vivo** (`negocio.json`, fuente Postgres):
   - Ventas/monto por región (barras).
   - Throughput de eventos (time series).
   - Monto total acumulado (stat).
   - Últimas transacciones (tabla).
   - Panel de texto con el símil cloud.

## 4. Mapa "local → nube" (se muestra en paneles de texto)

| Pieza local | GCP | AWS | Azure |
|---|---|---|---|
| Grafana | Cloud Monitoring | CloudWatch Dashboards | Azure Monitor (Workbooks) |
| Prometheus | Managed Service for Prometheus | Amazon Managed Prometheus | Azure Monitor managed Prometheus |
| cAdvisor (métricas de contenedor) | métricas de GKE | métricas de ECS/EKS | métricas de AKS |
| kafka-exporter (lag/throughput) | métricas de Pub/Sub | Kinesis / MSK | Event Hubs |
| Postgres (serving layer) | BigQuery | Redshift | Synapse |
| Spark Structured Streaming | Dataflow | Kinesis Data Analytics | Stream Analytics |

## 5. Mini-ejercicio (notebook)

`notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb`:
- Explica el stack y el símil cloud.
- Guía: levantar `completo`, abrir Grafana, arrancar el generador + el job de
  streaming, ver los dashboards moverse.
- **Ejercicio 1:** agregar un panel SQL "top 5 productos por monto" al dashboard
  de Negocio (se da la query parcial).
- **Ejercicio 2:** agregar un panel con una métrica de cAdvisor (p. ej. memoria
  del contenedor de Kafka) al dashboard de Infraestructura.

## 6. Archivos a crear / modificar

```
docker-compose.yml                              (+4 servicios en profile completo, +volúmenes, +puertos)
docker/prometheus/prometheus.yml
docker/grafana/provisioning/datasources/datasources.yml
docker/grafana/provisioning/dashboards/dashboards.yml
docker/grafana/provisioning/dashboards/infra.json
docker/grafana/provisioning/dashboards/negocio.json
docker/postgres/initdb/01_analytics.sql
scripts/streaming_a_postgres.py
notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb
README.md                                       (puertos, símil cloud, cómo usarlo)
```

## 7. Puertos nuevos

| Servicio | Puerto host | Notas |
|---|---|---|
| Grafana | 3000 | UI principal (acceso anónimo lectura) |
| Prometheus | 9090 | opcional, para enseñar PromQL |
| cAdvisor | — | interno |
| kafka-exporter | — | interno |

Sin conflictos con los puertos actuales (8888/8890, 4040, 9092, 9083, 10000, 10002).

## 8. Recursos

~1 GB adicional. Con `completo` (~3–4 GB) + observabilidad (~1 GB) ≈ 5 GB, entra
en los 8 GB de Colima. Si queda ajustado, subir Colima a 10 GB
(`colima stop && colima start --cpu 4 --memory 10`). Documentar en README.

## 9. Validación (criterios de aceptación)

Antes de dar por terminado, validar end-to-end:
1. `docker compose --profile completo up -d --build` levanta los 10 contenedores
   sin crash-loops.
2. Prometheus: todos los targets en estado `up` (cadvisor, kafka-exporter).
3. Grafana: datasources Prometheus y Postgres "OK"; los 2 dashboards cargan.
4. Dashboard Infraestructura muestra CPU/RAM reales y lag de Kafka.
5. Tras correr generador + `streaming_a_postgres.py`, el dashboard Negocio
   muestra datos reales y se actualiza.
6. Notebook `07_observabilidad_grafana.ipynb` ejecuta sin error las celdas base.

## 10. Riesgos y mitigaciones

- **cAdvisor en Colima:** los mounts pueden no exponer todas las métricas.
  Mitigación: validar temprano; si falla, fallback a node-exporter / métricas
  degradadas.
- **initdb sólo en volumen limpio:** entornos ya instalados no tendrán la DB
  `analytics`. Mitigación: `CREATE TABLE IF NOT EXISTS` en el job + nota en README.
- **Job de streaming es de larga duración:** para clase se corre manualmente
  (o con `trigger(processingTime)`), y se documenta cómo detenerlo.
- **Spark→Postgres lentitud de arranque:** igual que el resto de Spark en este
  entorno (~1 min de init); aceptable para demo.

## 11. Fuera de alcance (YAGNI)

- Alertas/alerting de Grafana.
- Métricas JVM internas de Spark (efímeras; se cubre con cAdvisor del contenedor).
- Autenticación/usuarios reales en Grafana (es entorno educativo local).
- Persistencia histórica larga en Prometheus (retención por defecto).
