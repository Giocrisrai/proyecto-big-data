#!/usr/bin/env python3
"""
Generador de datos de streaming para practicas de Big Data.
Produce eventos simulados hacia Kafka o archivos JSONL/CSV.

Uso:
    # Modo Kafka (requiere perfil completo de Docker)
    python generar_datos_streaming.py --tipo transacciones --velocidad 5 --duracion 60
    python generar_datos_streaming.py --tipo logs --velocidad 20 --duracion 120
    python generar_datos_streaming.py --tipo iot --velocidad 10 --duracion 60

    # Modo archivo
    python generar_datos_streaming.py --tipo transacciones --archivo datos/streaming/transacciones.jsonl --cantidad 1000
    python generar_datos_streaming.py --tipo clickstream --archivo datos/streaming/clicks.csv --cantidad 500

    # Modo archivo con problemas de calidad
    python generar_datos_streaming.py --tipo iot --archivo datos/streaming/iot_sucia.jsonl --cantidad 500 --calidad baja
"""

import argparse
import csv
import json
import random
import time
import sys
from datetime import datetime, timedelta


# =============================================================================
# CONFIGURACION DE PATRONES Y DISTRIBUCIONES
# =============================================================================

SEMILLA = 42
random.seed(SEMILLA)

# --- Transacciones ---
PRODUCTOS = [
    ("Laptop Gaming", 800000, 2500000),
    ("Laptop Oficina", 350000, 900000),
    ("Mouse Inalambrico", 8000, 35000),
    ("Teclado Mecanico", 35000, 120000),
    ("Monitor 27\" 4K", 250000, 700000),
    ("Monitor 24\" FullHD", 120000, 280000),
    ("Audifonos Bluetooth", 18000, 150000),
    ("Webcam HD", 25000, 95000),
    ("Disco SSD 1TB", 60000, 180000),
    ("Disco SSD 500GB", 35000, 90000),
    ("Memoria RAM 32GB", 55000, 130000),
    ("Memoria RAM 16GB", 30000, 70000),
    ("Cable USB-C", 3000, 12000),
    ("Cargador USB-C", 12000, 45000),
    ("Hub USB 7 puertos", 18000, 55000),
    ("Mousepad", 5000, 15000),
    ("Silla Gamer", 200000, 650000),
    ("Escritorio Electrico", 300000, 800000),
    ("Lampara LED", 15000, 40000),
    ("Micrófono USB", 35000, 120000),
]
METODOS_PAGO = ["tarjeta_credito", "tarjeta_debito", "transferencia", "efectivo", "paypal"]
TIENDAS = list(range(1, 46))
REGIONES = {
    1: "Norte", 2: "Norte", 3: "Norte",
    4: "Centro", 5: "Centro", 6: "Centro", 7: "Centro", 8: "Centro",
    9: "Sur", 10: "Sur", 11: "Sur",
}

# --- Logs Web ---
ENDPOINTS = [
    "/api/productos", "/api/usuarios", "/api/ventas", "/api/login",
    "/api/carrito", "/api/checkout", "/api/buscar", "/api/categorias",
    "/api/reportes", "/api/inventario", "/api/auth/token", "/api/notificaciones",
    "/", "/health", "/static/js/app.js", "/static/css/main.css",
    "/api/promociones", "/api/historial", "/api/devoluciones",
]
METODOS_HTTP = ["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE", "PATCH"]
STATUS_CODES = [200, 200, 200, 200, 200, 201, 204, 301, 302, 400, 401, 403, 404, 429, 500, 502, 503]
USER_AGENTS = [
    "Chrome/120", "Chrome/121", "Firefox/121", "Firefox/122",
    "Safari/17", "Safari/17.2", "Edge/120", "Edge/121",
    "Opera/106", "Mozilla/5.0 (compatible; Googlebot/2.1)",
]
IPS_PRIVADAS = [f"192.168.{a}.{b}" for a in range(1, 11) for b in range(1, 255)]
IPS_PUBLICAS = [f"10.0.{a}.{b}" for a in range(1, 6) for b in range(1, 50)]

# --- IoT ---
SENSORES = [f"sensor_{i:03d}" for i in range(1, 51)]
UBICACIONES = [
    "bodega_norte", "bodega_sur", "bodega_central", "bodega_oriente",
    "planta_1", "planta_2", "planta_3",
    "oficina_central", "oficina_norte", "oficina_sur",
    "exterior_patio", "exterior_techo", "exterior_jardin",
    "sala_servidores", "sala_servidores_2", "data_center",
]
TIPOS_SENSOR = {
    "temperatura": {"unidad": "°C", "min": -10, "max": 60, "prec": 1},
    "humedad": {"unidad": "%", "min": 0, "max": 100, "prec": 1},
    "presion": {"unidad": "hPa", "min": 980, "max": 1040, "prec": 0},
    "viento": {"unidad": "km/h", "min": 0, "max": 80, "prec": 1},
    "luminosidad": {"unidad": "lux", "min": 0, "max": 10000, "prec": 0},
}

# --- Clickstream ---
PAGINAS = [
    "/", "/productos", "/productos/laptops", "/productos/accesorios",
    "/productos/monitores", "/ofertas", "/nuevos-ingresos",
    "/carrito", "/checkout", "/login", "/registro", "/buscar",
    "/cuenta", "/cuenta/pedidos", "/cuenta/favoritos",
    "/ayuda", "/contacto", "/blog", "/blog/ofertas-verano",
]
ACCIONES = ["vista", "click", "scroll", "hover", "submit", "salir"]

# --- Stock Market ---
EMPRESAS = [
    ("AAPL", "Apple Inc.", 180, 250),
    ("GOOGL", "Alphabet Inc.", 140, 200),
    ("MSFT", "Microsoft Corp.", 350, 450),
    ("AMZN", "Amazon.com Inc.", 150, 220),
    ("META", "Meta Platforms", 350, 550),
    ("TSLA", "Tesla Inc.", 180, 400),
    ("NVDA", "NVIDIA Corp.", 600, 950),
    ("JPM", "JPMorgan Chase", 150, 220),
    ("V", "Visa Inc.", 240, 300),
    ("CODELCO", "Codelco", 3500, 5000),
    ("FALABELLA", "Falabella", 2500, 4000),
    ("COPEC", "Empresas Copec", 6000, 9000),
]

# --- Redes Sociales ---
USUARIOS = [f"usuario_{i:04d}" for i in range(1, 201)]
HASHTAGS = [
    "#bigdata", "#datascience", "#python", "#spark", "#kafka",
    "#cloud", "#aws", "#gcp", "#azure", "#machinelearning",
    "#ai", "#deeptech", "#startup", "#innovation", "#techchile",
    "#santiago", "#latinamerica", "#remoto", "#programacion",
]
EMOCIONES = ["positivo", "neutro", "negativo"]
PLATAFORMAS = ["Twitter", "LinkedIn", "Instagram", "Facebook", "TikTok", "Threads"]


# =============================================================================
# PATRONES DE COMPORTAMIENTO
# =============================================================================

def factor_estacional(hora_del_dia, dia_semana):
    """Factor multiplicativo segun hora y dia (1.0 = normal)."""
    factor_hora = {
        0: 0.1, 1: 0.05, 2: 0.03, 3: 0.02, 4: 0.02, 5: 0.05,
        6: 0.1, 7: 0.2,
        8: 0.6, 9: 0.9, 10: 1.0, 11: 1.0,
        12: 0.8, 13: 0.9,
        14: 1.0, 15: 1.0, 16: 0.9, 17: 0.8,
        18: 0.7, 19: 0.6, 20: 0.5, 21: 0.4, 22: 0.3, 23: 0.2,
    }
    # Fin de semana baja actividad comercial
    factor_dia = 0.6 if dia_semana >= 5 else 1.0
    return factor_hora.get(hora_del_dia, 0.5) * factor_dia


def generar_timestamp(base=datetime.now(), variacion_seg=0):
    """Genera timestamp con posible variacion."""
    if variacion_seg > 0:
        offset = random.randint(-variacion_seg, 0)
        t = base + timedelta(seconds=offset)
    else:
        t = base
    return t.isoformat()


# =============================================================================
# GENERADORES DE EVENTOS
# =============================================================================

def generar_transaccion(seq_id, hora_base=None, dia_base=None, **kwargs):
    """Genera un evento de transaccion de venta con patrones estacionales."""
    producto, precio_min, precio_max = random.choice(PRODUCTOS)
    monto = random.randint(precio_min, precio_max)
    tienda_id = random.choice(TIENDAS)
    region = REGIONES.get(tienda_id, "Centro")

    # A veces agregar envio
    tiene_envio = random.random() < 0.3
    costo_envio = random.randint(2000, 8000) if tiene_envio else 0

    calidad = kwargs.get("calidad", "alta")
    if calidad == "baja":
        if random.random() < 0.05:
            monto = None
        if random.random() < 0.03:
            producto = None

    evento = {
        "id": f"tx_{seq_id:06d}",
        "tipo_evento": "transaccion",
        "timestamp": datetime.now().isoformat(),
        "monto": monto,
        "costo_envio": costo_envio,
        "total": (monto or 0) + costo_envio,
        "tienda_id": tienda_id,
        "region": region,
        "producto": producto,
        "cantidad": random.randint(1, 5) if calidad != "baja" or random.random() > 0.05 else -1,
        "metodo_pago": random.choice(METODOS_PAGO),
        "cliente_id": random.randint(1000, 9999),
    }
    return evento


def generar_log(seq_id, hora_base=None, dia_base=None, **kwargs):
    """Genera un evento de log de servidor web con patrones de carga."""
    endpoint = random.choices(ENDPOINTS, weights=[
        5, 3, 4, 3, 2, 2, 2, 1, 1, 1, 2, 1,
        3, 1, 2, 1, 1, 1, 1,
    ])[0]

    if "checkout" in endpoint or "ventas" in endpoint:
        status = random.choices(STATUS_CODES, weights=[80, 5, 2, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1])[0]
    else:
        status = random.choices(STATUS_CODES, weights=[60, 10, 5, 5, 3, 2, 1, 2, 1, 2, 2, 2, 3, 2, 1, 1, 1])[0]

    base_time = 30 if status == 200 else 200 if status < 400 else 500 if status < 500 else 2000
    response_time = max(1, int(random.gauss(base_time, base_time * 0.5)))

    calidad = kwargs.get("calidad", "alta")
    if calidad == "baja":
        if random.random() < 0.04:
            response_time = 99999

    evento = {
        "id": f"log_{seq_id:06d}",
        "tipo_evento": "log_web",
        "timestamp": datetime.now().isoformat(),
        "ip": random.choice(IPS_PRIVADAS + IPS_PUBLICAS),
        "endpoint": endpoint,
        "method": random.choice(METODOS_HTTP),
        "status_code": status,
        "response_time_ms": response_time,
        "user_agent": random.choice(USER_AGENTS),
        "bytes_enviados": random.randint(200, 50000),
    }
    return evento


def generar_iot(seq_id, hora_base=None, dia_base=None, **kwargs):
    """Genera un evento de sensor IoT con patrones ambientales."""
    sensor_id = random.choice(SENSORES)
    ubicacion = random.choice(UBICACIONES)

    temp_base = 22.0
    if "bodega" in ubicacion:
        temp_base = 18.0
    elif "exterior" in ubicacion:
        temp_base = max(8.0, 20.0 - abs((datetime.now().hour or 12) - 14) * 1.5)
    elif "sala" in ubicacion or "data" in ubicacion:
        temp_base = 24.0

    calidad = kwargs.get("calidad", "alta")
    temp = round(random.gauss(temp_base, 3.0), 1)
    humedad = round(random.gauss(55.0 if "exterior" not in ubicacion else 65.0, 15.0), 1)
    presion = round(random.gauss(1013.0, 5.0), 0)

    if calidad == "baja":
        if random.random() < 0.05:
            temp = round(random.gauss(99.0, 1.0), 1)  # Sensor dañado
        if random.random() < 0.02:
            humedad = -1
        if random.random() < 0.03:
            sensor_id = None

    evento = {
        "id": f"iot_{seq_id:06d}",
        "tipo_evento": "lectura_iot",
        "timestamp": datetime.now().isoformat(),
        "sensor_id": sensor_id,
        "tipo_sensor": "multisensor",
        "temperatura": temp,
        "humedad": humedad,
        "presion_hpa": presion,
        "ubicacion": ubicacion,
        "bateria": round(random.uniform(20.0, 100.0), 1),
        "senal_dbm": random.randint(-90, -30),
    }
    return evento


def generar_clickstream(seq_id, hora_base=None, dia_base=None, **kwargs):
    """Genera un evento de clickstream de navegacion web."""
    usuario = random.choice(USUARIOS)
    pagina_origen = random.choice(PAGINAS)
    pagina_destino = random.choice(PAGINAS)

    sesion_id = f"sess_{random.randint(10000, 99999)}_{seq_id % 1000:03d}"

    evento = {
        "id": f"click_{seq_id:06d}",
        "tipo_evento": "clickstream",
        "timestamp": datetime.now().isoformat(),
        "usuario_id": usuario,
        "sesion_id": sesion_id,
        "pagina_origen": pagina_origen,
        "pagina_destino": pagina_destino,
        "accion": random.choice(ACCIONES),
        "tiempo_pagina_s": round(random.gauss(30, 20), 1) if random.random() > 0.1 else 0.5,
        "dispositivo": random.choice(["mobile", "desktop", "tablet"]),
        "navegador": random.choice(USER_AGENTS),
    }
    return evento


def generar_stock_tick(seq_id, hora_base=None, dia_base=None, **kwargs):
    """Genera un tick de precio de acciones bursatiles."""
    simbolo, nombre, precio_base, precio_max = random.choice(EMPRESAS)
    cambio_pct = random.gauss(0.0, 0.5)
    precio_actual = round(precio_base * (1 + cambio_pct / 100), 2)

    tendencia = kwargs.get("tendencia", "estable")
    if tendencia == "alcista":
        precio_actual *= 1 + random.uniform(0, 0.3) / 100
    elif tendencia == "bajista":
        precio_actual *= 1 - random.uniform(0, 0.5) / 100

    cambio_absoluto = round(precio_actual - precio_base, 2)

    evento = {
        "id": f"stock_{seq_id:06d}",
        "tipo_evento": "stock_tick",
        "timestamp": datetime.now().isoformat(),
        "simbolo": simbolo,
        "nombre_empresa": nombre,
        "precio_usd": precio_actual,
        "cambio_diario_pct": round(cambio_pct, 2),
        "cambio_diario_usd": cambio_absoluto,
        "volumen": random.randint(10000, 5000000),
        "mercado": random.choice(["NYSE", "NASDAQ", "BCS"]),
    }
    return evento


def generar_social_post(seq_id, hora_base=None, dia_base=None, **kwargs):
    """Genera un evento de publicacion en redes sociales."""
    usuario = random.choice(USUARIOS)

    n_hashtags = random.randint(1, 4)
    tags = random.sample(HASHTAGS, n_hashtags)

    evento = {
        "id": f"social_{seq_id:06d}",
        "tipo_evento": "social_post",
        "timestamp": datetime.now().isoformat(),
        "usuario_id": usuario,
        "plataforma": random.choice(PLATAFORMAS),
        "tipo_contenido": random.choice(["texto", "imagen", "video", "enlace", "encuesta"]),
        "emocion": random.choice(EMOCIONES),
        "hashtags": tags,
        "likes": int(random.expovariate(1 / 50)),
        "retweets": int(random.expovariate(1 / 10)),
        "respuestas": int(random.expovariate(1 / 5)),
        "alcance_estimado": random.randint(100, 50000),
    }
    return evento


# =============================================================================
# REGISTRO DE GENERADORES
# =============================================================================

GENERADORES = {
    "transacciones": generar_transaccion,
    "logs": generar_log,
    "iot": generar_iot,
    "clickstream": generar_clickstream,
    "stock": generar_stock_tick,
    "social": generar_social_post,
}

DESCRIPCIONES = {
    "transacciones": "Ventas en tiendas con productos, montos y metodos de pago",
    "logs": "Registros de servidor web con endpoints, status codes y tiempos",
    "iot": "Lecturas de sensores con temperatura, humedad y presion",
    "clickstream": "Navegacion de usuarios en sitio web (paginas, clics, sesiones)",
    "stock": "Ticks de precios bursatiles en tiempo real",
    "social": "Publicaciones en redes sociales con hashtags y metricas",
}

MODO_KAFKA_COMPATIBLE = {"transacciones", "logs", "iot"}


# =============================================================================
# FUNCIONES DE ENVIO
# =============================================================================

def enviar_a_kafka(tipo, velocidad, duracion, topic, **kwargs):
    """Envia eventos a Kafka."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("ERROR: kafka-python-ng no instalado. Ejecuta: pip install kafka-python-ng")
        sys.exit(1)

    if tipo not in MODO_KAFKA_COMPATIBLE:
        print(f"ERROR: Tipo '{tipo}' no soportado en modo Kafka.")
        print(f"Tipos compatibles: {', '.join(sorted(MODO_KAFKA_COMPATIBLE))}")
        print(f"Usa --archivo para generar datos de tipo '{tipo}' a archivo.")
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers="kafka:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    generador = GENERADORES[tipo]
    intervalo = 1.0 / velocidad
    fin = time.time() + duracion
    seq = 0
    hora_base = datetime.now().hour
    dia_base = datetime.now().weekday()

    print(f"\n{'='*60}")
    print(f"  Generador: {tipo}")
    print(f"  Velocidad: {velocidad} eventos/seg")
    print(f"  Duracion:  {duracion}s")
    print(f"  Topic:     {topic}")
    print(f"{'='*60}")
    print("  Presiona Ctrl+C para detener\n")

    try:
        while time.time() < fin:
            seq += 1
            evento = generador(seq, hora_base=hora_base, dia_base=dia_base, **kwargs)
            producer.send(topic, value=evento)
            if seq % 50 == 0:
                print(f"  Enviados: {seq} eventos", end="\r")
            time.sleep(intervalo)
    except KeyboardInterrupt:
        print("\n  Detenido por el usuario")
    finally:
        producer.flush()
        producer.close()
        print(f"\n  Total enviados: {seq} eventos al topic '{topic}'")


def escribir_a_archivo(tipo, cantidad, archivo, formato="jsonl", **kwargs):
    """Escribe eventos a un archivo (JSONL o CSV)."""
    generador = GENERADORES[tipo]

    print(f"\n{'='*60}")
    print(f"  Generador: {tipo}")
    print(f"  Cantidad:  {cantidad} eventos")
    print(f"  Archivo:   {archivo}")
    print(f"  Formato:   {formato}")
    if kwargs.get("calidad") == "baja":
        print("  Calidad:   BAJA (con errores y anomalias)")
    if kwargs.get("tendencia"):
        print(f"  Tendencia: {kwargs['tendencia']}")
    print(f"{'='*60}\n")

    if formato == "csv":
        _escribir_csv(archivo, tipo, cantidad, generador, **kwargs)
    else:
        _escribir_jsonl(archivo, tipo, cantidad, generador, **kwargs)


def _escribir_jsonl(archivo, tipo, cantidad, generador, **kwargs):
    """Escribe en formato JSONL (un JSON por linea)."""
    with open(archivo, "w") as f:
        for i in range(1, cantidad + 1):
            evento = generador(i, **kwargs)
            f.write(json.dumps(evento, ensure_ascii=False) + "\n")
            if i % 500 == 0:
                print(f"  Generados: {i}/{cantidad}", end="\r")
    print(f"\n  Archivo generado: {archivo} ({cantidad} eventos)")


def _escribir_csv(archivo, tipo, cantidad, generador, **kwargs):
    """Escribe en formato CSV con cabecera."""
    primer_evento = generador(1, **kwargs)
    columnas = list(primer_evento.keys())

    with open(archivo, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columnas)
        writer.writeheader()
        writer.writerow(primer_evento)
        for i in range(2, cantidad + 1):
            evento = generador(i, **kwargs)
            writer.writerow(evento)
            if i % 500 == 0:
                print(f"  Generados: {i}/{cantidad}", end="\r")
    print(f"\n  Archivo generado: {archivo} ({cantidad} eventos en CSV)")


# =============================================================================
# INTERFAZ DE LINEA DE COMANDOS
# =============================================================================

def main():
    descripciones = "\n".join(f"    {k:15s} {v}" for k, v in DESCRIPCIONES.items())

    parser = argparse.ArgumentParser(
        description="Generador de datos de streaming para Big Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Tipos disponibles:
{descripciones}

Ejemplos:
  # Generar 1000 transacciones a archivo JSONL
  python generar_datos_streaming.py --tipo transacciones --archivo datos/streaming/tx.jsonl --cantidad 1000

  # Generar datos IoT con calidad baja (datos sucios)
  python generar_datos_streaming.py --tipo iot --archivo datos/streaming/iot_sucio.jsonl --cantidad 500 --calidad baja

  # Generar ticks de stock con tendencia alcista
  python generar_datos_streaming.py --tipo stock --archivo datos/streaming/stock_bull.csv --cantidad 2000

  # Enviar a Kafka (solo tipos: transacciones, logs, iot)
  python generar_datos_streaming.py --tipo transacciones --velocidad 10 --duracion 120
        """
    )
    parser.add_argument("--tipo", choices=list(GENERADORES.keys()),
                        required=True, help="Tipo de datos a generar")

    # Modo Kafka
    parser.add_argument("--velocidad", type=int, default=10,
                        help="Eventos por segundo (modo Kafka, default: 10)")
    parser.add_argument("--duracion", type=int, default=60,
                        help="Duracion en segundos (modo Kafka, default: 60)")
    parser.add_argument("--topic", type=str, default=None,
                        help="Topic de Kafka (default: bigdata-<tipo>)")

    # Modo archivo
    parser.add_argument("--archivo", type=str, default=None,
                        help="Ruta del archivo de salida")
    parser.add_argument("--cantidad", type=int, default=1000,
                        help="Cantidad de eventos (default: 1000)")
    parser.add_argument("--formato", choices=["jsonl", "csv"], default="jsonl",
                        help="Formato de archivo (default: jsonl)")

    # Calidad de datos
    parser.add_argument("--calidad", choices=["alta", "baja"], default="alta",
                        help="Calidad de datos. 'baja' incluye errores, nulos y anomalias (default: alta)")

    # Patrones
    parser.add_argument("--tendencia", choices=["estable", "alcista", "bajista"],
                        default="estable", help="Tendencia de los datos (default: estable)")

    args = parser.parse_args()

    kwargs = {
        "calidad": args.calidad,
        "tendencia": args.tendencia,
    }

    if args.archivo:
        escribir_a_archivo(args.tipo, args.cantidad, args.archivo, args.formato, **kwargs)
    else:
        topic = args.topic or f"bigdata-{args.tipo}"
        enviar_a_kafka(args.tipo, args.velocidad, args.duracion, topic, **kwargs)


if __name__ == "__main__":
    main()
