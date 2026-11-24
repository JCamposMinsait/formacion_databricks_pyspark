"""
Pipeline declarativo (Lakeflow Spark Declarative Pipelines) · Telemetría eólica

Declaramos qué tablas queremos y Databricks decide el orden, gestiona los checkpoints,
reintenta los fallos y guarda las métricas de calidad.

Configuración que espera el pipeline (Settings → Configuration):
  ruta_landing   → /Volumes/<catálogo>/<esquema>/landing/telemetria_stream
  tabla_maestro  → <catálogo>.<esquema>.silver_dim_turbina   (la del lab 3)
El pipeline escribe en el esquema <esquema>_pipeline (ver README_lab4.md).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RUTA_LANDING = spark.conf.get("ruta_landing")
TABLA_MAESTRO = spark.conf.get("tabla_maestro")


# ---------------------------------------------------------------- BRONZE: los ficheros tal cual, con Auto Loader
@dp.table(name="bronze_telemetria", comment="Telemetría cruda, tal cual llega del simulador")
def bronze_telemetria():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaHints", "value DOUBLE, ts TIMESTAMP")
            .load(RUTA_LANDING)
            .select("*", F.col("_metadata.file_path").alias("_source_file")))


# ---------------------------------------------------------------- SILVER: reglas de calidad
REGLAS = {
    "valor_presente": "value IS NOT NULL",
    "sin_centinela": "value <> -9999",
    "turbina_valida": "turbine_id RLIKE '^WTG-[0-9]{4}$'",
    "calidad_ok": "quality = 'GOOD'",
}


@dp.table(name="silver_telemetria", comment="Telemetría limpia y sin duplicados")
@dp.expect_all_or_drop(REGLAS)                                              # la fila que no cumple se descarta y se cuenta
@dp.expect("potencia_razonable", "NOT (signal = 'power_kw' AND value > 6000)")   # solo avisa
def silver_telemetria():
    return (spark.readStream.table("bronze_telemetria")
            .withWatermark("ts", "5 minutes")
            .dropDuplicatesWithinWatermark(["turbine_id", "ts", "signal"])
            .select("turbine_id", "park_id", "ts", "signal", "value", "quality"))


# ---------------------------------------------------------------- GOLD: una fila por turbina y minuto, con su parque
@dp.materialized_view(name="gold_turbina_minuto", comment="Una fila por turbina y minuto, con una columna por señal")
def gold_turbina_minuto():
    maestro = spark.read.table(TABLA_MAESTRO).select("turbine_id", "parque", "modelo")
    return (spark.read.table("silver_telemetria")
            .groupBy("turbine_id", F.date_trunc("minute", "ts").alias("minuto"))
            .pivot("signal", ["wind_speed", "power_kw", "rotor_rpm", "temp_gearbox_c", "vibration_mm_s"])
            .agg(F.avg("value"))
            .join(maestro, "turbine_id", "left"))
