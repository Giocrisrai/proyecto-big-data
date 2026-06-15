# Guia de Instalacion - Entorno Big Data (BIY7131)

Guia paso a paso para configurar el entorno de practica de Big Data en tu equipo.

---

## Tabla de Contenidos

1. [Requisitos del sistema](#1-requisitos-del-sistema)
2. [Paso 0: Verificar requisitos previos](#2-paso-0-verificar-requisitos-previos)
3. [Paso 1: Instalar Docker Desktop](#3-paso-1-instalar-docker-desktop)
4. [Paso 2: Instalar Git](#4-paso-2-instalar-git)
5. [Paso 3: Fork y clonar el repositorio](#5-paso-3-fork-y-clonar-el-repositorio)
6. [Paso 4: Configurar el entorno](#6-paso-4-configurar-el-entorno)
7. [Paso 5: Levantar el perfil basico](#7-paso-5-levantar-el-perfil-basico)
8. [Paso 6: Verificar el entorno](#8-paso-6-verificar-el-entorno)
9. [Paso 7: Levantar el perfil completo](#9-paso-7-levantar-el-perfil-completo-cuando-se-necesite)
10. [Apendice A: Configurar recursos en Docker Desktop](#10-apendice-a-configurar-recursos-en-docker-desktop)
11. [Apendice B: Desinstalar todo](#11-apendice-b-desinstalar-todo)

---

## 1. Requisitos del sistema

| Sistema Operativo | RAM Minima | RAM Recomendada | Disco Libre | Software Previo |
|-------------------|------------|-----------------|-------------|-----------------|
| **Windows 10/11** (Pro o Home con WSL2) | 4 GB | 8 GB | 5 GB | WSL2, Docker Desktop, Git |
| **macOS 12+** (Intel y Apple Silicon) | 4 GB | 8 GB | 5 GB | Docker Desktop, Git |
| **Ubuntu 20.04+** / **Fedora 38+** | 4 GB | 8 GB | 5 GB | Docker Desktop o Docker Engine, Git |

> **Nota:** Los 4-8 GB de RAM son los que Docker Desktop necesita tener asignados. Tu equipo deberia tener al menos 8 GB de RAM total (16 GB recomendado).

---

## 2. Paso 0: Verificar requisitos previos

### Windows

1. **Verificar version de Windows:**
   - Presiona `Win + R`, escribe `winver` y verifica que tienes Windows 10 version 2004+ o Windows 11

2. **Instalar WSL2:**
   ```powershell
   # Abrir PowerShell como Administrador
   wsl --install
   ```
   Reinicia el equipo tras la instalacion.

3. **Verificar que WSL2 esta funcionando:**
   ```powershell
   wsl --status
   ```
   Debe mostrar "Default Version: 2"

4. **Verificar virtualizacion en BIOS:**
   - Abre el Administrador de Tareas (`Ctrl + Shift + Esc`)
   - Ve a la pestana "Rendimiento" > CPU
   - Verifica que "Virtualizacion: Habilitado" aparece
   - Si no esta habilitado, entra a la BIOS y activa VT-x (Intel) o AMD-V (AMD)

### macOS

1. **Verificar version de macOS:**
   ```bash
   sw_vers
   ```
   Necesitas macOS 12 (Monterey) o superior.

2. **Verificar si tienes chip Apple Silicon o Intel:**
   ```bash
   uname -m
   ```
   - `arm64` = Apple Silicon (M1, M2, M3, M4)
   - `x86_64` = Intel

### Linux

1. **Verificar version del kernel:**
   ```bash
   uname -r
   ```
   Necesitas kernel 4.15+ (Ubuntu 20.04+ lo cumple).

2. **Verificar soporte de cgroups v2:**
   ```bash
   stat -fc %T /sys/fs/cgroup/
   ```
   Debe mostrar `cgroup2fs`. Si muestra `tmpfs`, tienes cgroups v1 (aun funciona, pero es mas antiguo).

---

## 3. Paso 1: Instalar Docker Desktop

### Windows

1. Descarga Docker Desktop desde:
   https://docs.docker.com/desktop/install/windows-install/

2. Ejecuta el instalador y sigue las instrucciones
3. Asegurate de que "Use the WSL 2 based engine" este marcado
4. Reinicia el equipo si se solicita
5. Abre Docker Desktop y espera a que se inicie (icono verde en la barra de tareas)

### macOS

1. Descarga Docker Desktop desde:
   https://docs.docker.com/desktop/install/mac-install/

   > Selecciona la version correcta: **Apple Silicon** (M1/M2/M3/M4) o **Intel**

2. Abre el archivo `.dmg` y arrastra Docker a Aplicaciones
3. Abre Docker Desktop desde Aplicaciones
4. Acepta los terminos y espera a que se inicie (icono de ballena en la barra de menu)

### Linux

1. Descarga Docker Desktop desde:
   https://docs.docker.com/desktop/install/linux/

   O instala Docker Engine directamente:
   ```bash
   # Ubuntu/Debian
   sudo apt update
   sudo apt install docker.io docker-compose-plugin
   sudo usermod -aG docker $USER
   # Cierra sesion y vuelve a abrir
   ```

### Configurar recursos (todos los sistemas)

Una vez instalado Docker Desktop, asigna los recursos necesarios:

1. Abre Docker Desktop
2. Ve a **Settings** (icono de engranaje)
3. Ve a **Resources** > **Advanced** (o **Resources** directamente en Mac)
4. Configura:
   - **Memory:** 6 GB (minimo 4 GB)
   - **CPUs:** 4 (minimo 2)
   - **Disk image size:** 20 GB minimo
5. Haz clic en **Apply & Restart**

### Verificar la instalacion

```bash
docker --version
docker-compose --version
```

Ambos comandos deben mostrar la version instalada sin errores.

---

## 4. Paso 2: Instalar Git

### Windows

1. Descarga Git desde:
   https://git-scm.com/download/win

2. Ejecuta el instalador con las opciones por defecto
3. Verifica en una nueva terminal (Git Bash o PowerShell):
   ```bash
   git --version
   ```

### macOS

Opcion A - Xcode Command Line Tools (recomendado):
```bash
xcode-select --install
```

Opcion B - Homebrew:
```bash
brew install git
```

Verifica:
```bash
git --version
```

### Linux

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install git

# Fedora
sudo dnf install git
```

Verifica:
```bash
git --version
```

### Configurar Git (todos los sistemas)

```bash
git config --global user.name "Tu Nombre"
git config --global user.email "tu.email@duocuc.cl"
```

---

## 5. Paso 3: Fork y clonar el repositorio

### 5.1 Hacer Fork en GitHub

1. Ve a https://github.com/Giocrisrai/proyecto-big-data
2. Haz clic en el boton **Fork** (arriba a la derecha)
3. Selecciona tu cuenta como destino del fork
4. Espera a que se cree la copia en tu cuenta

### 5.2 Clonar tu fork

```bash
# Reemplaza TU_USUARIO con tu nombre de usuario de GitHub
git clone https://github.com/TU_USUARIO/proyecto-big-data.git
cd proyecto-big-data
```

### 5.3 Verificar el remote

```bash
git remote -v
```

Debe mostrar:
```
origin    https://github.com/TU_USUARIO/proyecto-big-data.git (fetch)
origin    https://github.com/TU_USUARIO/proyecto-big-data.git (push)
```

---

## 6. Paso 4: Configurar el entorno

### 6.1 Copiar el archivo de configuracion

```bash
cp .env.example .env
```

### 6.2 Variables de entorno

El archivo `.env` contiene las siguientes variables que puedes ajustar segun tu equipo:

| Variable | Valor por defecto | Descripcion |
|----------|-------------------|-------------|
| `SPARK_WORKER_MEMORY` | `2g` | Memoria asignada al worker de Spark |
| `SPARK_WORKER_CORES` | `2` | Nucleos de CPU para el worker de Spark |
| `SPARK_MASTER_WEBUI_PORT` | `8080` | Puerto de la interfaz web del Spark Master |
| `SPARK_WORKER_WEBUI_PORT` | `8081` | Puerto de la interfaz web del Spark Worker |
| `JUPYTER_TOKEN` | `bigdata2026` | Token de acceso a JupyterLab |
| `JUPYTER_PORT` | `8888` | Puerto de JupyterLab |
| `KAFKA_PORT` | `9092` | Puerto del broker Kafka |
| `POSTGRES_USER` | `hive` | Usuario de PostgreSQL (Hive Metastore) |
| `POSTGRES_PASSWORD` | `hive_metastore` | Contrasena de PostgreSQL |
| `POSTGRES_DB` | `metastore` | Base de datos de PostgreSQL |

> **Tip:** Si el puerto 8888 esta en uso, cambia `JUPYTER_PORT` a otro valor (ej: `8889`).

> **Tip:** Si tu equipo tiene poca RAM, reduce `SPARK_WORKER_MEMORY` a `1g`.

---

## 7. Paso 5: Levantar el perfil basico

El perfil basico incluye JupyterLab, Spark Master y Spark Worker. Es suficiente para EA1, la mayor parte de EA2 y las actividades extras.

```bash
docker-compose --profile basico up -d
```

### Primera ejecucion

La primera vez, Docker descargara las imagenes necesarias (~3 GB). Esto puede tardar entre 5 y 15 minutos dependiendo de tu conexion a internet.

Puedes ver el progreso con:
```bash
docker-compose logs -f
```

Presiona `Ctrl + C` para salir de los logs sin detener los servicios.

### Verificar que los servicios estan corriendo

```bash
docker-compose ps
```

Deberias ver algo como:
```
NAME                    STATUS
bigdata-jupyter         Up
bigdata-spark-master    Up
bigdata-spark-worker    Up
```

Si algun servicio muestra `Restarting` o `Exit`, revisa los logs:
```bash
docker-compose logs jupyter-spark
docker-compose logs spark-master
```

---

## 8. Paso 6: Verificar el entorno

### 8.1 Acceder a JupyterLab

Abre tu navegador y ve a:

```
http://localhost:8888?token=bigdata2026
```

> Si cambiaste `JUPYTER_PORT` o `JUPYTER_TOKEN` en `.env`, usa los valores que configuraste.

Deberias ver la interfaz de JupyterLab con las carpetas `notebooks/`, `datos/` y `scripts/`.

### 8.2 Verificar Spark UI

Abre en otra pestana:

```
http://localhost:8080
```

Deberias ver la interfaz web del Spark Master con al menos 1 worker registrado.

### 8.3 Ejecutar el script de verificacion

En JupyterLab, abre un notebook nuevo (Python 3) y ejecuta:

```python
%run /home/jovyan/scripts/verificar_entorno.py
```

El script verificara automaticamente:

- Version de Python (>= 3.10)
- PySpark 3.5.x instalado
- Librerias Python necesarias (pandas, numpy, matplotlib, seaborn, openpyxl, plotly)
- Datasets accesibles en `/home/jovyan/datos/`
- Capacidad de Spark para leer CSV y escribir/leer Parquet
- Conexion a Kafka (solo perfil completo)
- Conexion a Hive Metastore (solo perfil completo)
- Base Postgres `analytics` y tabla `ventas_agg` (solo perfil completo)
- Grafana accesible (solo perfil completo)

**Resultado esperado con perfil basico:** 7/11 verificaciones pasadas.

**Resultado esperado con perfil completo:** 11/11 verificaciones pasadas.

---

## 9. Paso 7: Levantar el perfil completo (cuando se necesite)

El perfil completo agrega Kafka, Zookeeper, PostgreSQL, Hive Metastore y HiveServer2. Se necesita para:

- **EA2 Notebook 04:** `hive_metastore.ipynb`
- **EA3 completa:** Todos los notebooks de procesamiento en tiempo real

```bash
# Si ya tienes el perfil basico corriendo, detienelo primero
docker-compose --profile basico down

# Levanta el perfil completo
docker-compose --profile completo up -d
```

### Esperar a que los servicios se inicialicen

Algunos servicios tardan en estar listos:

- **Kafka:** ~30 segundos despues de que el contenedor inicie
- **Hive Metastore:** ~1-2 minutos (necesita que PostgreSQL este listo primero)
- **HiveServer2:** ~1-2 minutos (necesita que Hive Metastore este listo)

Verifica que todo esta corriendo:
```bash
docker-compose ps
```

Todos los servicios deben mostrar estado `Up`.

### Grafana y dashboards en vivo (EA3 notebook 07)

El perfil completo incluye **Grafana**, **Prometheus**, **cAdvisor** y **kafka-exporter**.

1. Abre Grafana en el navegador:
   ```
   http://localhost:3000
   ```
   (o el puerto configurado en `GRAFANA_PORT` dentro de `.env`)

2. En la carpeta **Big Data** verás dos dashboards:
   - **Negocio en vivo** — KPIs, throughput, ventas por región
   - **Infraestructura** — CPU/RAM, salud de servicios, Kafka

3. Para alimentar el dashboard de negocio con datos en tiempo real:

   ```bash
   # Linux / macOS
   ./scripts/iniciar_dashboard_vivo.sh

   # Windows
   scripts\iniciar_dashboard_vivo.bat
   ```

4. Para detener el pipeline:

   ```bash
   ./scripts/detener_dashboard_vivo.sh          # Linux / macOS
   scripts\detener_dashboard_vivo.bat           # Windows
   ```

> Guía detallada de datos sintéticos (batch y streaming):
> [docs/guias/generacion_datos_sinteticos.md](docs/guias/generacion_datos_sinteticos.md)

### Verificar Kafka

```bash
# Entrar al contenedor de Kafka
docker exec -it bigdata-kafka bash

# Listar topics (puede estar vacio al inicio)
kafka-topics.sh --list --bootstrap-server localhost:29092

# Salir del contenedor
exit
```

---

## 10. Apendice A: Configurar recursos en Docker Desktop

### Windows

1. Abre Docker Desktop
2. Ve a **Settings** > **Resources** > **WSL Integration**
3. Activa la integracion con tu distribucion WSL (Ubuntu)
4. Ve a **Resources** > **Advanced**:
   - **Memory:** 6 GB (8 GB si tienes 16 GB de RAM)
   - **CPUs:** 4
   - **Swap:** 2 GB
5. Haz clic en **Apply & Restart**

> **Nota para Windows Home:** Si usas Docker con WSL2, la asignacion de memoria se controla desde WSL. Crea o edita el archivo `%UserProfile%\.wslconfig`:
> ```ini
> [wsl2]
> memory=6GB
> processors=4
> ```
> Luego reinicia WSL: `wsl --shutdown`

### macOS

1. Abre Docker Desktop
2. Haz clic en el icono de engranaje (**Settings**)
3. Ve a **Resources**:
   - **Memory:** 6 GB (8 GB si tienes 16 GB de RAM)
   - **CPUs:** 4
   - **Swap:** 2 GB
   - **Disk image size:** 20 GB minimo
4. Haz clic en **Apply & Restart**

### Linux

Si usas Docker Desktop, la configuracion es similar a macOS.

Si usas Docker Engine directamente, los recursos se controlan desde el sistema operativo. Docker usara los recursos disponibles del sistema.

---

## 11. Apendice B: Desinstalar todo

Si necesitas liberar espacio o desinstalar el entorno:

### Paso 1: Detener y eliminar contenedores y volumenes

```bash
cd proyecto-big-data

# Detener todos los servicios y eliminar volumenes
docker-compose --profile completo down -v
```

### Paso 2: Eliminar imagenes descargadas

```bash
# Eliminar TODAS las imagenes, contenedores y caches de Docker
# ADVERTENCIA: esto afecta a TODOS tus proyectos Docker, no solo este
docker system prune -a
```

> **Atencion:** `docker system prune -a` elimina TODO lo de Docker en tu equipo. Si tienes otros proyectos con Docker, elimina solo las imagenes de este proyecto:
> ```bash
> docker rmi bitnami/spark:3.5 bitnami/kafka:3.7 bitnami/zookeeper:3.9
> docker rmi apache/hive:4.0.1 postgres:16-alpine
> docker rmi $(docker images --filter "reference=proyecto-big-data*" -q)
> ```

### Paso 3: Eliminar el repositorio (opcional)

```bash
cd ..
rm -rf proyecto-big-data
```

### Paso 4: Desinstalar Docker Desktop (opcional)

- **Windows:** Panel de Control > Programas > Desinstalar Docker Desktop
- **macOS:** Mover Docker.app a la Papelera, luego vaciarla
- **Linux:** `sudo apt remove docker-desktop` o seguir la guia oficial

---

<p align="center">
  <strong>DUOC UC</strong> | Big Data (BIY7131) | Semestre 2026-1
</p>
