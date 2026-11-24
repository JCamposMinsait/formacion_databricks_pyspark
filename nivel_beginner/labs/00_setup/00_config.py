# Databricks notebook source
# Configuración común. Se carga con %run desde los labs y las demos.
# Crea (si no existen) el catálogo, el esquema de cada usuario y el volumen,
# y define las rutas y los maestros de parques y aerogeneradores.

import re
from pyspark.sql import functions as F, types as T

CATALOGO = "formacion"          # si no se puede crear, se usa el catálogo por defecto de la cuenta
VOLUMEN  = "landing"            # volumen para ficheros crudos

_usuario = spark.sql("SELECT current_user()").first()[0]
ALUMNO   = re.sub(r"[^a-z0-9]", "_", _usuario.split("@")[0].lower())
ESQUEMA  = f"eolica_{ALUMNO}"

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
except Exception as e:
    if spark.sql(f"SHOW CATALOGS LIKE '{CATALOGO}'").count() == 0:
        # Free Edition puede no dejar crear catálogos: usamos el que trae la cuenta
        CATALOGO = spark.sql("SELECT current_catalog()").first()[0]
        print(f"No se ha podido crear el catálogo formacion. Se usa {CATALOGO}. Detalle: {str(e)[:120]}")
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ESQUEMA} COMMENT 'Esquema de laboratorio de {_usuario}'")
spark.sql(f"USE SCHEMA {ESQUEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUMEN} COMMENT 'Ficheros crudos del curso'")

# Día 4: el pipeline declarativo escribe en su propio esquema. En el vuestro ya hay tablas bronze_telemetria
# y silver_telemetria de los días 1 y 2, y un pipeline no puede crear tablas con nombres que ya existen.
ESQUEMA_PIPELINE = f"{ESQUEMA}_pipeline"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA_PIPELINE} COMMENT 'Tablas del pipeline declarativo de {_usuario}'")

RUTA_VOL        = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"
RUTA_RAW_BATCH  = f"{RUTA_VOL}/telemetria_raw"      # día 1: carga masiva
RUTA_RAW_STREAM = f"{RUTA_VOL}/telemetria_stream"   # día 4: simulador cada 10 s
RUTA_CHECKPOINT = f"{RUTA_VOL}/_checkpoints"
RUTA_SCHEMAS    = f"{RUTA_VOL}/_schemas"

def t(nombre: str) -> str:
    """Nombre completo (3 niveles) de una tabla en el esquema del alumno."""
    return f"{CATALOGO}.{ESQUEMA}.{nombre}"

# COMMAND ----------

# Maestros: 6 parques y 240 aerogeneradores. P01 tiene el 60 % de las máquinas (se usa en el día 2 para ver data skew).

PARQUES = [
    # park_id, nombre, provincia, lat, lon, operador
    ("P01", "Sierra Norte",   "Huelva",   37.95, -6.75, "EnerSur"),
    ("P02", "Campo de Níjar", "Almería",  36.97, -2.20, "EnerSur"),
    ("P03", "Tarifa Levante", "Cádiz",    36.05, -5.65, "Viento Atlántico"),
    ("P04", "Páramo Alto",    "Burgos",   42.55, -3.60, "Viento Atlántico"),
    ("P05", "Muela de Borja", "Zaragoza", 41.80, -1.55, "Cierzo Energía"),
    ("P06", "Costa da Morte", "A Coruña", 43.20, -9.00, "Cierzo Energía"),
]

MODELOS = [
    # modelo, potencia nominal kW, diámetro rotor m
    ("AE-4200", 4200, 150),
    ("BX-4500", 4500, 149),
    ("CW-5000", 5000, 145),
]

def df_parques():
    return spark.createDataFrame(PARQUES, "park_id string, nombre string, provincia string, lat double, lon double, operador string")

def df_turbinas(n=240, seed=42):
    """240 turbinas; P01 se queda con el 60 % (skew a propósito)."""
    import random
    random.seed(seed)
    filas = []
    for i in range(1, n + 1):
        park = "P01" if i <= int(n * 0.6) else random.choice(["P02", "P03", "P04", "P05", "P06"])
        modelo, kw, rotor = random.choice(MODELOS)
        anio = random.randint(2012, 2025)
        filas.append((f"WTG-{i:04d}", park, modelo, kw, rotor, f"{anio}-{random.randint(1,12):02d}-01"))
    return (spark.createDataFrame(filas, "turbine_id string, park_id string, modelo string, potencia_nominal_kw int, diametro_rotor_m int, fecha_puesta_marcha string")
                 .withColumn("fecha_puesta_marcha", F.to_date("fecha_puesta_marcha")))

print(f"Config cargada: catálogo {CATALOGO}, esquema {ESQUEMA}, esquema del pipeline {ESQUEMA_PIPELINE}, volumen {RUTA_VOL}")
