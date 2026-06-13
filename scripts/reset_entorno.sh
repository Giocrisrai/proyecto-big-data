#!/bin/bash
# Reset completo del entorno Big Data
# Detiene contenedores, borra volumenes y datos temporales

echo "============================================"
echo "  Reset del Entorno Big Data"
echo "============================================"
echo ""

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

echo "1. Deteniendo contenedores..."
docker compose --profile completo down -v 2>/dev/null
docker compose --profile basico down -v 2>/dev/null

echo "2. Eliminando datos temporales..."
rm -rf datos/streaming/output/ 2>/dev/null
rm -rf datos/tmp/ 2>/dev/null

echo "3. Limpiando checkpoints de Spark..."
rm -rf spark-warehouse/ metastore_db/ derby.log 2>/dev/null

echo ""
echo "Reset completado. Para volver a iniciar:"
echo "  docker compose --profile completo up -d --build"
echo "  ./scripts/iniciar_dashboard_vivo.sh"
echo ""
