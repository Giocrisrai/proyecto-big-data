@echo off
REM Reset completo del entorno Big Data
REM Detiene contenedores, borra volumenes y datos temporales

echo ============================================
echo   Reset del Entorno Big Data
echo ============================================
echo.

cd /d "%~dp0\.."

echo 1. Deteniendo contenedores...
docker compose --profile completo down -v 2>nul
docker compose --profile basico down -v 2>nul

echo 2. Eliminando datos temporales...
if exist datos\streaming\output rd /s /q datos\streaming\output
if exist datos\tmp rd /s /q datos\tmp

echo 3. Limpiando checkpoints de Spark...
if exist spark-warehouse rd /s /q spark-warehouse
if exist metastore_db rd /s /q metastore_db
if exist derby.log del derby.log

echo.
echo Reset completado. Para volver a iniciar:
echo   docker compose --profile completo up -d --build
echo   scripts\iniciar_dashboard_vivo.bat
echo.
pause
