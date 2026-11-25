"""
Lab 4c · Pipeline declarativo
Día 4 · Declaras qué tablas quieres · Streaming tables y vistas materializadas · Expectations · AUTO CDC
Configuración del pipeline: ruta_landing y tabla_maestro. Esquema de destino: eolica_<alumno>_pipeline (ver README_lab4.md).
Rellena lo marcado con TODO. Solución: labs/soluciones/dia4/lab4c_pipeline_declarativo.py
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RUTA_LANDING  = spark.conf.get("ruta_landing")
TABLA_MAESTRO = spark.conf.get("tabla_maestro")

# ---------------------------------------------------------------- BRONZE (completo)
@dp.table(name="bronze_telemetria", comment="Telemetría cruda EAV (Auto Loader)", cluster_by=["turbine_id"])
def bronze_telemetria():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "value DOUBLE, ts TIMESTAMP")
            .load(RUTA_LANDING)
            .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at")))

# ---------------------------------------------------------------- SILVER
# TODO: cuatro reglas que descarten la fila si no se cumplen:
#   valor presente · sin centinela -9999 · turbine_id con formato WTG-0000 · quality = GOOD
REGLAS = {
    "valor_presente": "___",
    "sin_centinela":  "___",
    "turbina_valida": "___",
    "calidad_ok":     "___",
}

@dp.table(name="silver_telemetria", comment="Telemetría limpia y deduplicada", cluster_by=["turbine_id", "ts"])
@dp.expect_all_or_drop(REGLAS)
@dp.expect("potencia_razonable", "NOT (signal = 'power_kw' AND value > 6000)")   # solo avisa
def silver_telemetria():
    return (spark.readStream.table("bronze_telemetria")
            .withWatermark("ts", "5 minutes")
            .dropDuplicatesWithinWatermark(___)        # TODO: clave de la lectura
            .select("turbine_id", "park_id", "ts", "signal", "value", "quality"))

# ---------------------------------------------------------------- DIMENSIÓN SCD2
dp.create_streaming_table("dim_turbina", comment="Maestro de turbinas con historia (SCD tipo 2)")
# TODO: AUTO CDC desde la foto del maestro, clave turbine_id, SCD tipo 2
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_turbina",
    source=___,
    keys=___,
    stored_as_scd_type=___,
)

# ---------------------------------------------------------------- GOLD
@dp.materialized_view(name="gold_turbina_minuto", comment="Una fila por turbina y minuto")
def gold_turbina_minuto():
    s = spark.read.table("silver_telemetria")
    dim = spark.read.table("dim_turbina").filter(___)      # TODO: solo la versión actual (__END_AT nulo)
    wide = (s.groupBy("turbine_id", "park_id", F.date_trunc("minute", "ts").alias("minuto"))
              .pivot("signal", ["wind_speed", "power_kw", "rotor_rpm", "temp_gearbox_c", "vibration_mm_s"])
              .agg(F.avg("value")))
    return wide.join(dim.select("turbine_id", "modelo", "potencia_nominal_kw", "estado"), "turbine_id", "left")

# TODO (reto): vista materializada gold_parque_hora con energía (MWh) y turbinas reportando por parque y hora
