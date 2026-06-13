@echo off
REM Inicia el pipeline completo para alimentar el dashboard de Grafana en vivo.
REM Uso: scripts\iniciar_dashboard_vivo.bat [duracion_seg] [velocidad_tx_s]
setlocal

set DURACION=%1
set VELOCIDAD=%2
if "%DURACION%"=="" set DURACION=3600
if "%VELOCIDAD%"=="" set VELOCIDAD=5

cd /d "%~dp0\.."

echo ============================================================
echo   Pipeline Grafana en vivo
echo   Generador: %VELOCIDAD% tx/s durante %DURACION%s
echo ============================================================

docker ps --format "{{.Names}}" | findstr /x "bigdata-jupyter" >nul
if errorlevel 1 (
  echo ERROR: bigdata-jupyter no esta corriendo.
  echo Levanta el stack: docker compose --profile completo up -d
  exit /b 1
)

echo ^>^> Verificando base analytics...
docker exec bigdata-postgres psql -U hive -tAc "SELECT 1 FROM pg_database WHERE datname='analytics';" 2>nul | findstr /x "1" >nul
if errorlevel 1 (
  docker exec -i bigdata-postgres psql -U hive -d postgres < docker\postgres\initdb\01_analytics.sql
) else (
  echo ^>^> Base analytics OK
  docker exec bigdata-postgres psql -U hive -d analytics -c "CREATE TABLE IF NOT EXISTS ventas_agg (region TEXT, n_tx BIGINT, monto_total BIGINT, actualizado_en TIMESTAMP DEFAULT now());" >nul
)

echo ^>^> Deteniendo procesos previos...
docker exec bigdata-jupyter pkill -f streaming_a_postgres.py 2>nul
docker exec bigdata-jupyter pkill -f generar_datos_streaming.py 2>nul
timeout /t 2 /nobreak >nul

echo ^>^> Iniciando Spark Streaming...
docker exec -d bigdata-jupyter python /home/jovyan/scripts/streaming_a_postgres.py

echo ^>^> Esperando que Spark inicie (~30s)...
timeout /t 30 /nobreak >nul

echo ^>^> Iniciando generador de transacciones...
docker exec -d bigdata-jupyter python /home/jovyan/scripts/generar_datos_streaming.py --tipo transacciones --velocidad %VELOCIDAD% --duracion %DURACION% --topic transacciones

set GRAFANA_PORT=3000
if exist .env (
  for /f "tokens=2 delims==" %%a in ('findstr /r "^GRAFANA_PORT=" .env') do set GRAFANA_PORT=%%a
)

echo.
echo ============================================================
echo   Pipeline iniciado. Abre Grafana:
echo   http://localhost:%GRAFANA_PORT%/d/bigdata-negocio
echo.
echo   Dashboards:
echo     Negocio:  http://localhost:%GRAFANA_PORT%/d/bigdata-negocio
echo     Infra:    http://localhost:%GRAFANA_PORT%/d/bigdata-infra
echo.
echo   Para detener:
echo     docker exec bigdata-jupyter pkill -f streaming_a_postgres
echo     docker exec bigdata-jupyter pkill -f generar_datos_streaming
echo ============================================================
