@echo off
REM Detiene el pipeline de Grafana en vivo (Spark + generador).
echo ^>^> Deteniendo pipeline Grafana...
docker exec bigdata-jupyter pkill -f streaming_a_postgres.py 2>nul
docker exec bigdata-jupyter pkill -f generar_datos_streaming.py 2>nul
echo ^>^> Pipeline detenido.
