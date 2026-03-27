FROM quay.io/jupyter/pyspark-notebook:latest

# Instalar uv (gestor de paquetes ultrarapido de Astral)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copiar e instalar dependencias Python con uv (10-100x mas rapido que pip)
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

# Asegurar que PySpark y py4j esten en el PYTHONPATH
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV UV_SYSTEM_PYTHON=1

# Directorio de trabajo
WORKDIR /home/jovyan
