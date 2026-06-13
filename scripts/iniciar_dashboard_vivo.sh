#!/usr/bin/env bash
# Inicia el pipeline completo para alimentar el dashboard de Grafana en vivo.
# Uso: ./scripts/iniciar_dashboard_vivo.sh
set -euo pipefail

DURACION="${1:-3600}"   # segundos del generador (default: 1 hora)
VELOCIDAD="${2:-5}"     # transacciones por segundo

echo "============================================================"
echo "  Pipeline Grafana en vivo"
echo "  Generador: ${VELOCIDAD} tx/s durante ${DURACION}s"
echo "============================================================"

# 1. Verificar que el stack completo está arriba
if ! docker ps --format '{{.Names}}' | grep -q '^bigdata-jupyter$'; then
  echo "ERROR: bigdata-jupyter no está corriendo."
  echo "Levanta el stack: docker compose --profile completo up -d"
  exit 1
fi

# 2. Crear base analytics si no existe (entornos ya instalados)
if ! docker exec bigdata-postgres psql -U hive -lqt | grep -q 'analytics'; then
  echo ">> Creando base 'analytics' y tabla ventas_agg..."
  docker exec -i bigdata-postgres psql -U hive -d postgres < docker/postgres/initdb/01_analytics.sql
else
  echo ">> Base 'analytics' OK"
fi

# 3. Detener procesos previos del pipeline (si existen)
echo ">> Deteniendo procesos previos del pipeline..."
docker exec bigdata-jupyter pkill -f 'streaming_a_postgres.py' 2>/dev/null || true
docker exec bigdata-jupyter pkill -f 'generar_datos_streaming.py' 2>/dev/null || true
sleep 2

# 4. Lanzar job Spark Streaming (background dentro del contenedor)
echo ">> Iniciando Spark Streaming (Kafka -> Postgres)..."
docker exec -d bigdata-jupyter python /home/jovyan/scripts/streaming_a_postgres.py

# 5. Esperar a que Spark arranque
echo ">> Esperando que Spark inicie (~30s)..."
sleep 30

# 6. Lanzar generador de transacciones (background dentro del contenedor)
echo ">> Iniciando generador de transacciones (${VELOCIDAD} tx/s, ${DURACION}s)..."
docker exec -d bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py \
  --tipo transacciones --velocidad "${VELOCIDAD}" --duracion "${DURACION}" --topic transacciones

# 7. Mostrar URLs
GRAFANA_PORT="${GRAFANA_PORT:-3000}"
if [ -f .env ]; then
  GRAFANA_PORT=$(grep -E '^GRAFANA_PORT=' .env 2>/dev/null | cut -d= -f2 || echo "${GRAFANA_PORT}")
fi

echo ""
echo "============================================================"
echo "  Pipeline iniciado. Abre Grafana:"
echo "  http://localhost:${GRAFANA_PORT}/d/bigdata-negocio"
echo ""
echo "  Dashboards disponibles:"
echo "    Negocio:  http://localhost:${GRAFANA_PORT}/d/bigdata-negocio"
echo "    Infra:    http://localhost:${GRAFANA_PORT}/d/bigdata-infra"
echo ""
echo "  Para detener:"
echo "    docker exec bigdata-jupyter pkill -f streaming_a_postgres"
echo "    docker exec bigdata-jupyter pkill -f generar_datos_streaming"
echo "============================================================"
