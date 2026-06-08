# Repositorio de Practica - Big Data (BIY7131)

![DUOC UC](https://img.shields.io/badge/DUOC_UC-BIY7131-003DA5?style=flat-square)
![Semestre](https://img.shields.io/badge/Semestre-2026--1-green?style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.9-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Hive](https://img.shields.io/badge/Apache_Hive-4.0-FDEE21?style=flat-square&logo=apachehive&logoColor=black)
![Grafana](https://img.shields.io/badge/Grafana-11.4-F46800?style=flat-square&logo=grafana&logoColor=white)

## Descripcion

Repositorio de practica para la asignatura **Big Data (BIY7131)** de DUOC UC. Contiene un entorno Docker completamente configurado con Apache Spark, Kafka y Hive, junto con notebooks de Jupyter organizados por Experiencia de Aprendizaje (EA), datasets de ejemplo y material teorico.

Esta pensado para que cada estudiante haga **fork** del repositorio, lo clone en su equipo y trabaje localmente con Docker, sin necesidad de instalar dependencias adicionales.

---

## Inicio Rapido

```bash
# 1. Haz fork del repositorio en GitHub (boton "Fork" arriba a la derecha)

# 2. Clona TU fork (reemplaza TU_USUARIO con tu nombre de usuario de GitHub)
git clone https://github.com/TU_USUARIO/proyecto-big-data.git
cd proyecto-big-data

# 3. Copia el archivo de configuracion
cp .env.example .env

# 4. Levanta el entorno basico (Jupyter + Spark)
docker-compose --profile basico up -d

# 5. Abre JupyterLab en tu navegador
#    http://localhost:8888?token=bigdata2026
```

> La primera vez Docker descargara las imagenes (~3 GB). Esto puede tardar varios minutos segun tu conexion a internet.

---

## Requisitos

| Requisito | Minimo | Recomendado |
|-----------|--------|-------------|
| **Docker Desktop** | 4.0+ | Ultima version |
| **Git** | 2.30+ | Ultima version |
| **RAM disponible** | 4 GB | 8 GB |
| **Espacio en disco** | ~5 GB | ~8 GB |
| **Sistema operativo** | Windows 10+, macOS 12+, Linux | - |

> Para instrucciones detalladas de instalacion paso a paso, consulta [GUIA_INSTALACION.md](GUIA_INSTALACION.md).

---

## Estructura del Repositorio

```
proyecto-big-data/
├── docker-compose.yml              # Orquestacion de servicios
├── .env.example                    # Variables de entorno (copiar a .env)
├── docker/
│   └── jupyter-spark/
│       ├── Dockerfile              # Imagen personalizada de Jupyter + Spark
│       └── requirements.txt        # Dependencias Python
├── notebooks/                      # Notebooks del alumno (con celdas para completar)
│   ├── EA1_fundamentos/            # Experiencia de Aprendizaje 1
│   ├── EA2_etl_batch/              # Experiencia de Aprendizaje 2
│   ├── EA3_tiempo_real/            # Experiencia de Aprendizaje 3
│   └── extras/                     # Actividades complementarias
├── datos/                          # Datasets de ejemplo
│   ├── flights.csv
│   ├── sales.csv
│   ├── stores.csv
│   ├── netflix_titles.csv
│   ├── ufo_sightings.xlsx
│   ├── cartas_magic.csv
│   └── streaming/                  # Datos generados para streaming
│       ├── transacciones_1000.jsonl
│       ├── logs_1000.jsonl
│       ├── iot_1000.jsonl
│       ├── iot_dirty_500.jsonl      # Con anomalias intencionales
│       ├── clickstream_1000.jsonl
│       ├── stock_1000.jsonl
│       └── social_1000.jsonl
├── scripts/
│   ├── verificar_entorno.py        # Verificacion del entorno
│   ├── generar_datos_streaming.py  # Generador de datos en tiempo real
│   ├── reset_entorno.sh            # Reset del entorno (Linux/Mac)
│   └── reset_entorno.bat           # Reset del entorno (Windows)
├── docs/
│   ├── temas/                      # Material teorico (PDFs)
│   ├── guias/                      # Guias adicionales
│   └── referencias/                # Mapeo de labs Google Skills Boost
├── README.md                       # Este archivo
└── GUIA_INSTALACION.md             # Guia detallada de instalacion
```

---

## Guia por Experiencia de Aprendizaje

### Demo guiada: servicios locales vs nube (recomendado para empezar)

El notebook `notebooks/DEMO_servicios_vs_nube.ipynb` recorre los 4 servicios del
entorno (Spark, Kafka, Spark Structured Streaming y Hive) *funcionando*, y conecta
cada uno con su equivalente gestionado en la nube (Dataproc, Pub/Sub, Dataflow,
Glue/BigQuery, etc.). Ideal para la primera clase, para que los alumnos entiendan
qué hace cada pieza y el paralelo con AWS/Azure/GCP. Requiere el perfil `completo`.

### EA1: Fundamentos de Big Data (perfil `basico`)

| # | Notebook | Descripcion |
|---|----------|-------------|
| 01 | `introduccion_spark.ipynb` | Primeros pasos con Apache Spark y PySpark |
| 02 | `rdds_basico.ipynb` | Resilient Distributed Datasets (RDDs) |
| 03 | `dataframes_intro.ipynb` | Introduccion a DataFrames de Spark |
| 04 | `arquitecturas_bigdata.ipynb` | Arquitecturas Lambda, Kappa y componentes |

```bash
# Perfil necesario
docker-compose --profile basico up -d
```

### EA2: ETL y Procesamiento Batch (perfil `basico`)

| # | Notebook | Descripcion | Perfil |
|---|----------|-------------|--------|
| 01 | `ingesta_datos.ipynb` | Carga de datos desde diversas fuentes | basico |
| 02 | `transformacion_limpieza.ipynb` | Limpieza y transformacion de datos | basico |
| 03 | `spark_sql.ipynb` | Consultas SQL sobre DataFrames | basico |
| 04 | `hive_metastore.ipynb` | Hive Metastore (embebido) y tablas persistentes | basico |
| 05 | `machine_learning_spark.ipynb` | Machine Learning con Spark MLlib | basico |
| 06 | `visualizacion_datos.ipynb` | Visualizacion con Matplotlib, Seaborn y Plotly | basico |

```bash
# Todos los notebooks de EA2 corren con el perfil basico
# (el notebook 04 usa el metastore Hive embebido de Spark, no requiere el servicio externo)
docker-compose --profile basico up -d
```

### EA3: Procesamiento en Tiempo Real (perfil `completo`)

| # | Notebook | Descripcion | Perfil |
|---|----------|-------------|--------|
| 01 | `kafka_introduccion.ipynb` | Introduccion a Apache Kafka | completo |
| 02 | `ingesta_tiempo_real.ipynb` | Ingesta de datos en tiempo real con Kafka | completo |
| 03 | `spark_structured_streaming.ipynb` | Spark Structured Streaming | completo |
| 04 | `dashboard_tiempo_real.ipynb` | Dashboard interactivo con ipywidgets | **basico** |
| 05 | `streaming_sin_kafka.ipynb` | Streaming offline desde JSONL (sin Kafka) | **basico** |
| 06 | `watermarks_y_windows.ipynb` | Watermarks y ventanas (explicacion visual) | **basico** |
| 07 | `observabilidad_grafana.ipynb` | Observabilidad con Grafana + simil cloud | completo |

```bash
# Para notebooks 01-03 y 07 (con Kafka / Grafana)
docker-compose --profile completo up -d

# Para notebooks 04-06 (sin Kafka)
docker-compose --profile basico up -d
```

### Actividades Extras

| Notebook | Dataset | Descripcion |
|----------|---------|-------------|
| `actividad_vuelos.ipynb` | `flights.csv` | Analisis de datos de vuelos |
| `actividad_ventas.ipynb` | `sales.csv`, `stores.csv` | Analisis de ventas por tienda |
| `actividad_netflix.ipynb` | `netflix_titles.csv` | Analisis del catalogo de Netflix |
| `actividad_avistamientos.ipynb` | `ufo_sightings.xlsx` | Analisis de avistamientos OVNI |
| `actividad_cartas_magic.ipynb` | `cartas_magic.csv` | Analisis de cartas Magic: The Gathering |

---

## Comandos Utiles

```bash
# Levantar perfil basico (Jupyter + Spark)
docker-compose --profile basico up -d

# Levantar perfil completo (+ Kafka + Hive)
docker-compose --profile completo up -d

# Ver estado de los contenedores
docker-compose ps

# Ver logs de un servicio especifico
docker-compose logs -f jupyter-spark
docker-compose logs -f kafka

# Detener todos los servicios
docker-compose --profile completo down

# Detener y eliminar volumenes (borra datos persistentes)
docker-compose --profile completo down -v

# Reiniciar un servicio especifico
docker-compose restart jupyter-spark

# Reconstruir la imagen de Jupyter (tras cambios en Dockerfile)
docker-compose build jupyter-spark
```

---

## Puertos y URLs

| Servicio | Puerto | URL | Perfil |
|----------|--------|-----|--------|
| **JupyterLab** | 8888 | http://localhost:8888?token=bigdata2026 | basico, completo |
| **Spark UI** | 4040 | http://localhost:4040 | basico, completo |
| **Kafka** | 9092 | `localhost:9092` (broker) | completo |
| **Hive Metastore** | 9083 | `thrift://localhost:9083` | completo |
| **HiveServer2** | 10000 | `jdbc:hive2://localhost:10000` | completo |
| **HiveServer2 Web UI** | 10002 | http://localhost:10002 | completo |
| **Grafana** | 3000 | http://localhost:3000 (acceso anonimo de lectura) | completo |
| **Prometheus** | 9090 | http://localhost:9090 | completo |

> Si el puerto 3000 ya esta en uso en tu equipo, cambia `GRAFANA_PORT` en `.env`
> (por ejemplo `GRAFANA_PORT=3001`) y reinicia: `docker-compose --profile completo up -d`.

---

## Observabilidad con Grafana (servicios locales vs nube)

El perfil `completo` incluye un stack de observabilidad para **ver los servicios
funcionando en tiempo real** y entender su equivalente gestionado en la nube:

- **Prometheus** + **cAdvisor** + **kafka-exporter** recolectan métricas técnicas.
- **Grafana** las visualiza en dos dashboards pre-armados (carpeta *Big Data*):
  - **🩺 Infraestructura:** CPU/RAM por contenedor, estado up/down, lag de Kafka.
  - **📊 Negocio en vivo:** ventas por región/throughput, alimentado por el
    pipeline `Kafka → Spark Streaming → Postgres → Grafana`.

Cada dashboard incluye un panel con el **símil cloud** (Cloud Monitoring /
CloudWatch / Azure Monitor; Pub-Sub / Kinesis / Event Hubs; BigQuery / Redshift /
Synapse; Dataflow / Stream Analytics).

Para alimentar el dashboard de negocio: corré el notebook
`notebooks/EA3_tiempo_real/07_observabilidad_grafana.ipynb` (genera transacciones
y lanza el job `scripts/streaming_a_postgres.py`).

> **Recursos:** el perfil completo con observabilidad usa ~5-6 GB. Si Docker va
> justo, sube la memoria (con Colima: `colima stop && colima start --cpu 4 --memory 10`).
>
> **Entornos ya instalados:** la base `analytics` (que usa Grafana) se crea sola
> en una instalación limpia. Si ya tenías el entorno, créala una vez con:
> ```bash
> docker exec bigdata-postgres psql -U hive -d metastore -c "CREATE DATABASE analytics"
> docker exec bigdata-postgres psql -U hive -d analytics -c "CREATE TABLE IF NOT EXISTS ventas_agg (region TEXT, n_tx BIGINT, monto_total BIGINT, actualizado_en TIMESTAMP DEFAULT now());"
> ```

---

## Material Teorico

Los PDFs del curso se encuentran en `docs/temas/`:

| Tema | Archivo |
|------|---------|
| Presentacion de la asignatura | `Presentación de la asignatura.pdf` |
| Tema 1 | Introduccion a las tecnologias Big Data |
| Tema 2 | HDFS y MapReduce |
| Tema 3 | Spark I |
| Tema 4 | Spark II |
| Tema 5 | Spark III |
| Tema 6 | Apache Kafka |
| Tema 7 | Hive e Impala |
| Tema 8 | Cloud Computing - Conceptos generales |
| Tema 9 | Cloud Computing - Microsoft Azure |
| Tema 10 | Cloud Computing - AWS |
| Tema 11 | Cloud Computing - Google Cloud Platform |

Guias adicionales en `docs/guias/`:

- `despliegue_cluster_gcp.pdf` - Guia de despliegue de cluster en Google Cloud Platform
- `guia_visualizacion.pdf` - Guia de visualizacion de datos
- `uv_gestor_paquetes.md` - [Guia de uv](docs/guias/uv_gestor_paquetes.md): gestor de paquetes Python ultrarapido (Astral)

---

## Solucion de Problemas

### Docker no inicia en Windows

1. Verifica que WSL2 esta instalado: `wsl --status`
2. Habilita la virtualizacion en la BIOS (VT-x / AMD-V)
3. En Docker Desktop: Settings > General > "Use the WSL 2 based engine" debe estar activado

### Conflicto de puertos

Si el puerto 8888 u otro esta en uso:

```bash
# Edita .env y cambia el puerto
JUPYTER_PORT=8889

# Reinicia los servicios
docker-compose --profile basico down
docker-compose --profile basico up -d
```

### Problemas de memoria / servicios se reinician

```bash
# Verifica el uso de memoria
docker stats

# Ajusta la memoria del worker en .env
SPARK_WORKER_MEMORY=1g
```

En Docker Desktop: Settings > Resources > Memory, asigna al menos **4 GB** para el perfil basico y **6 GB** para el completo.

### Kafka no conecta

1. Verifica que estas usando el perfil **completo**: `docker-compose --profile completo up -d`
2. Espera ~1 minuto tras levantar los servicios para que Kafka se inicialice
3. Comprueba los logs: `docker-compose logs kafka`
4. Verifica que Zookeeper esta corriendo: `docker-compose ps zookeeper`

### Hive no responde

1. Espera ~2 minutos tras levantar el perfil completo
2. Verifica que PostgreSQL esta corriendo: `docker-compose ps postgres`
3. Revisa los logs: `docker-compose logs hive-metastore`

### JupyterLab no carga o da error 403

1. Asegurate de incluir el token en la URL: `http://localhost:8888?token=bigdata2026`
2. Si cambiaste el token en `.env`, usa el nuevo valor
3. Limpia la cache del navegador o abre en ventana de incognito

---

## Verificacion del Entorno

Dentro de JupyterLab, abre un notebook nuevo y ejecuta:

```python
%run /home/jovyan/scripts/verificar_entorno.py
```

Este script verifica:
- Python >= 3.10
- PySpark instalado y funcional (Spark >= 3.5; la imagen actual trae Spark 4.1)
- Librerias Python (pandas, numpy, matplotlib, seaborn, openpyxl, plotly)
- Datasets accesibles
- Lectura de CSV y Parquet con Spark
- Conexion a Kafka (perfil completo)
- Conexion a Hive Metastore (perfil completo)

Las verificaciones de Kafka y Hive solo pasaran si estas usando el perfil **completo**.

---

## Soluciones (para docentes)

Las versiones resueltas de todos los notebooks estan en el branch **`soluciones`**:

```bash
# Ver las soluciones (no necesario para alumnos)
git checkout soluciones
# Volver a main
git checkout main
```

> **Nota para alumnos:** Intenta resolver los ejercicios por tu cuenta antes de consultar las soluciones.

---

## Contribuir

Este repositorio esta pensado para que cada estudiante trabaje en su propio **fork**:

1. **No hagas Pull Requests** al repositorio original (a menos que el docente lo indique)
2. Trabaja en tu fork haciendo commits regulares
3. Si el repositorio original se actualiza, sincroniza tu fork:

```bash
# Agregar el repositorio original como remote (solo la primera vez)
git remote add upstream https://github.com/Giocrisrai/proyecto-big-data.git

# Traer cambios del original
git fetch upstream
git merge upstream/main
```

---

<p align="center">
  <strong>DUOC UC</strong> | Big Data (BIY7131) | Semestre 2026-1
</p>
