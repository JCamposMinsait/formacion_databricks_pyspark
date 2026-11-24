"""
Lakeflow Spark Declarative Pipeline (antes "Delta Live Tables") · Telemetría eólica
-------------------------------------------------------------------------------------
Declaramos QUÉ tablas queremos; el motor decide el orden (DAG), gestiona checkpoints,
reintentos, calidad de datos y el cómputo.

Configuración que espera el pipeline (key/value en la definición o en el bundle):
  ruta_landing   → /Volumes/<cat>/<esq>/landing/telemetria_stream
  tabla_maestro  → <cat>.<esq>.maestro_turbinas_snapshot

Equivalencia de APIs:  `from pyspark import pipelines as dp`  ⇔  `import dlt` (antigua, sigue funcionando)
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RUTA_LANDING  = spark.conf.get("ruta_landing")
TABLA_MAESTRO = spark.conf.get("tabla_maestro")

# ---------------------------------------------------------------- BRONZE
@dp.table(
    name="bronze_telemetria",
    comment="Telemetría cruda EAV tal cual llega del concentrador (Auto Loader)",
    cluster_by=["turbine_id"],
)
def bronze_telemetria():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaHints", "value DOUBLE, ts TIMESTAMP")
            .load(RUTA_LANDING)
            .select("*", F.col("_metadata.file_path").alias("_source_file"),
                    F.current_timestamp().alias("_ingested_at")))

# ---------------------------------------------------------------- SILVER
REGLAS = {
    "valor_presente":   "value IS NOT NULL",
    "sin_centinela":    "value <> -9999",
    "turbina_valida":   "turbine_id RLIKE '^WTG-[0-9]{4}$'",
    "calidad_ok":       "quality = 'GOOD'",
}

@dp.table(name="silver_telemetria", comment="Telemetría limpia y deduplicada", cluster_by=["turbine_id", "ts"])
@dp.expect_all_or_drop(REGLAS)                                   # fila que no cumple → fuera (y queda contada en métricas)
@dp.expect("potencia_razonable", "NOT (signal = 'power_kw' AND value > 6000)")   # solo avisa, no tira
def silver_telemetria():
    return (spark.readStream.table("bronze_telemetria")
            .withWatermark("ts", "5 minutes")
            .dropDuplicatesWithinWatermark(["turbine_id", "ts", "signal"])
            .select("turbine_id", "park_id", "ts", "signal", "value", "quality"))

# ---------------------------------------------------------------- DIMENSIÓN SCD2
# El maestro llega como "foto" completa en cada ejecución → AUTO CDC desde snapshot
dp.create_streaming_table("dim_turbina", comment="Maestro de turbinas con historia (SCD tipo 2)")
dp.create_auto_cdc_from_snapshot_flow(
    target="dim_turbina",
    source=TABLA_MAESTRO,
    keys=["turbine_id"],
    stored_as_scd_type=2,          # añade __START_AT / __END_AT
)

# ---------------------------------------------------------------- GOLD
@dp.materialized_view(name="gold_turbina_minuto", comment="Una fila por turbina y minuto (formato columnar)")
def gold_turbina_minuto():
    s = spark.read.table("silver_telemetria")
    dim = spark.read.table("dim_turbina").filter("__END_AT IS NULL")
    wide = (s.groupBy("turbine_id", "park_id", F.date_trunc("minute", "ts").alias("minuto"))
              .pivot("signal", ["wind_speed", "power_kw", "rotor_rpm", "temp_gearbox_c", "vibration_mm_s"])
              .agg(F.avg("value")))
    return wide.join(dim.select("turbine_id", "modelo", "potencia_nominal_kw", "estado"), "turbine_id", "left")

@dp.materialized_view(name="gold_parque_hora", comment="Energía y factor de capacidad por parque y hora")
def gold_parque_hora():
    return (spark.read.table("gold_turbina_minuto")
            .groupBy("park_id", F.date_trunc("hour", "minuto").alias("hora"))
            .agg(F.round(F.sum(F.col("power_kw") / 60) / 1000, 3).alias("energia_mwh"),
                 F.round(F.sum(F.col("power_kw") / 60) / F.sum(F.col("potencia_nominal_kw") / 60), 4).alias("factor_capacidad"),
                 F.countDistinct("turbine_id").alias("turbinas_reportando")))
