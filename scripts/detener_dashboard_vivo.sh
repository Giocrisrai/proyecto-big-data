#!/usr/bin/env bash
# Detiene el pipeline de Grafana en vivo (Spark + generador).
set -euo pipefail

echo ">> Deteniendo pipeline Grafana..."
docker exec bigdata-jupyter pkill -f 'streaming_a_postgres.py' 2>/dev/null || true
docker exec bigdata-jupyter pkill -f 'generar_datos_streaming.py' 2>/dev/null || true
echo ">> Pipeline detenido."
