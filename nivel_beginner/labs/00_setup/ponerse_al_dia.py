# Databricks notebook source
# MAGIC %md
# MAGIC # Ponerse al día
# MAGIC
# MAGIC Para quien se ha perdido un día o ha roto sus tablas. Elige el día que vas a empezar y el notebook crea en tu esquema las tablas que ese día necesita de los anteriores. **Solo crea lo que falta**, salvo que elijas rehacer.
# MAGIC
# MAGIC | Vas a empezar el día | Te crea |
# MAGIC |---|---|
# MAGIC | 2 | `bronze_telemetria` |
# MAGIC | 3 | lo anterior más `silver_telemetria`, `silver_cuarentena` y `silver_dim_turbina` |
# MAGIC | 4 o 5 | lo anterior más `dim_turbina_scd2`, la función `fn_potencia_teorica` y las tablas Gold |
# MAGIC
# MAGIC Es el mismo código que las soluciones de los labs, sin los ejercicios intermedios. Los maestros salen del notebook de configuración, no de la Postgres.
# MAGIC En serverless tarda entre 5 y 20 minutos según el día. Si vas justo de cuota, baja los días de telemetría.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("empiezo_el_dia", "3", ["2", "3", "4", "5"], "Voy a empezar el día")
dbutils.widgets.dropdown("rehacer", "no", ["no", "si"], "Rehacer aunque ya exista")
dbutils.widgets.text("dias_de_datos", "2", "Días de telemetría")

DIA = int(dbutils.widgets.get("empiezo_el_dia"))
REHACER = dbutils.widgets.get("rehacer") == "si"
DIAS = int(dbutils.widgets.get("dias_de_datos"))


def falta(*tablas):
    return REHACER or not all(spark.catalog.tableExists(t(x)) for x in tablas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Día 1: telemetría cruda y Bronze

# COMMAND ----------

import math, datetime as dt

if DIA >= 2 and falta("bronze_telemetria"):
    INICIO = dt.datetime(2026, 11, 2, 0, 0, 0, tzinfo=dt.timezone.utc)
    DEGRADADAS = ["WTG-0042", "WTG-0117", "WTG-0199"]
    CALIENTES = ["WTG-0013", "WTG-0187"]

    base = (df_turbinas().select("turbine_id", "park_id", "potencia_nominal_kw", "diametro_rotor_m")
            .crossJoin(spark.range(0, DIAS * 1440).withColumnRenamed("id", "m"))
            .withColumn("ts", F.timestamp_seconds(F.lit(int(INICIO.timestamp())) + F.col("m") * 60))
            .withColumn("park_bias", F.when(F.col("park_id") == "P03", 1.8)
                                      .when(F.col("park_id") == "P06", 1.2)
                                      .when(F.col("park_id") == "P02", -1.0).otherwise(0.0))
            .withColumn("wind_speed", F.greatest(F.lit(0.0),
                         F.lit(5.8) + F.col("park_bias")
                         + F.lit(3.0) * F.sin(F.lit(2 * math.pi) * (F.col("m") % 1440) / 1440)
                         + F.randn(seed=1) * 1.8))
            .withColumn("cp", F.when(F.col("turbine_id").isin(DEGRADADAS), 0.30).otherwise(0.40))
            .withColumn("power_kw", F.when((F.col("wind_speed") < 3) | (F.col("wind_speed") >= 25), F.lit(0.0))
                                     .otherwise(F.least(F.col("potencia_nominal_kw").cast("double"),
                                                        F.lit(0.5 * 1.225 * math.pi / 1000) * F.pow(F.col("diametro_rotor_m") / 2, 2)
                                                        * F.pow("wind_speed", 3) * F.col("cp")))
                        + F.randn(seed=2) * 40)
            .withColumn("rotor_rpm", F.least(F.lit(14.5), F.col("wind_speed") * 1.15) + F.randn(seed=3) * 0.2)
            .withColumn("temp_gearbox_c", F.lit(42.0) + F.col("power_kw") / F.col("potencia_nominal_kw") * 28 + F.randn(seed=4) * 1.5
                        + F.when(F.col("turbine_id").isin(CALIENTES), 8.0).otherwise(0.0))
            .withColumn("vibration_mm_s", F.abs(F.randn(seed=5)) * 0.6 + F.col("rotor_rpm") * 0.05))

    eav = (base.selectExpr(
              "turbine_id", "park_id", "ts",
              "stack(5, 'wind_speed', wind_speed, 'power_kw', power_kw, 'rotor_rpm', rotor_rpm, "
              "'temp_gearbox_c', temp_gearbox_c, 'vibration_mm_s', vibration_mm_s) as (signal, value)")
           .withColumn("value", F.when(F.rand(seed=6) < 0.002, F.lit(-9999.0))
                                 .when(F.rand(seed=7) < 0.003, F.lit(None).cast("double"))
                                 .otherwise(F.round("value", 3)))
           .withColumn("quality", F.when(F.rand(seed=8) < 0.01, "BAD").otherwise("GOOD"))
           .withColumn("fw_version", F.when(F.col("park_id").isin("P01", "P02"), "3.2.1").otherwise("3.1.0"))
           .withColumn("ts", F.date_format("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
           .withColumn("fecha", F.substring("ts", 1, 10)))

    duplicados = eav.sample(fraction=0.005, seed=9)
    huerfanas = (eav.filter(F.col("turbine_id").isin("WTG-0001", "WTG-0002"))
                 .withColumn("turbine_id", F.when(F.col("turbine_id") == "WTG-0001", "WTG-0901").otherwise("WTG-0902")))
    (eav.unionByName(duplicados).unionByName(huerfanas)
        .repartition(48).write.mode("overwrite").partitionBy("fecha").json(RUTA_RAW_BATCH))

    esquema_raw = "turbine_id STRING, park_id STRING, ts STRING, signal STRING, value DOUBLE, quality STRING, fw_version STRING"
    bronze = (spark.read.schema(esquema_raw).json(RUTA_RAW_BATCH)
              .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at")))
    spark.sql(f"DROP TABLE IF EXISTS {t('bronze_telemetria')}")
    bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("bronze_telemetria"))
    print("bronze_telemetria:", spark.table(t("bronze_telemetria")).count(), "filas")
else:
    print("Día 1: no hace falta crear nada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Día 3: maestro y Silver

# COMMAND ----------

if DIA >= 4 and falta("silver_telemetria", "silver_cuarentena", "silver_dim_turbina"):
    parques = df_parques()
    turbinas = df_turbinas().withColumn("estado", F.lit("OPERATIVA"))
    limpio = (spark.table(t("bronze_telemetria"))
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
        .withColumn("value", F.when((F.col("signal") == "power_kw") & (F.col("value") < 0), 0.0).otherwise(F.col("value")))
        .dropDuplicates(["turbine_id", "ts", "signal"]))
    maestro = (turbinas.select("turbine_id", "park_id", "modelo", "potencia_nominal_kw", "estado")
               .join(parques.select("park_id", F.col("nombre").alias("parque"), "provincia", "operador"), "park_id"))
    silver = limpio.join(F.broadcast(maestro.drop("park_id")), "turbine_id", "left")
    silver.filter(F.col("modelo").isNotNull()).drop("_source_file") \
          .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_telemetria"))
    silver.filter(F.col("modelo").isNull()) \
          .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_cuarentena"))
    maestro.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_dim_turbina"))
    print("silver_telemetria:", spark.table(t("silver_telemetria")).count(), "filas")
else:
    print("Silver: no hace falta crear nada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Día 3: columnar y Gold

# COMMAND ----------

if DIA >= 4 and falta("gold_kpi_turbina_dia", "gold_kpi_parque_dia", "dim_turbina_scd2"):
    spark.sql("""
    CREATE OR REPLACE TABLE silver_telemetria_wide
    CLUSTER BY (turbine_id, ts)
    AS
    SELECT * FROM (
      SELECT turbine_id, park_id, parque, modelo, potencia_nominal_kw, ts, signal, value
      FROM silver_telemetria
      WHERE quality = 'GOOD'
    )
    PIVOT (
      avg(value) FOR signal IN ('wind_speed' AS wind_speed, 'power_kw' AS power_kw, 'rotor_rpm' AS rotor_rpm,
                                'temp_gearbox_c' AS temp_gearbox_c, 'vibration_mm_s' AS vibration_mm_s)
    )""")

    spark.sql("""
    CREATE OR REPLACE TABLE silver_telemetria_enriquecida AS
    SELECT
      *,
      timestampdiff(MINUTE, LAG(ts) OVER w, ts) AS min_desde_anterior,
      COALESCE(power_kw, LAG(power_kw) OVER w, 0) AS power_kw_fill,
      power_kw - LAG(power_kw) OVER w AS rampa_kw,
      avg(temp_gearbox_c) OVER (PARTITION BY turbine_id ORDER BY ts ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS temp_gearbox_mm10,
      power_kw / NULLIF(potencia_nominal_kw, 0) AS ratio_carga,
      COALESCE(power_kw, LAG(power_kw) OVER w, 0) / 60.0 AS energia_kwh
    FROM silver_telemetria_wide
    WINDOW w AS (PARTITION BY turbine_id ORDER BY ts)""")

    # Potencia teórica con funciones nativas: mismo resultado que la Pandas UDF del lab 3
    RHO, CP = 1.225, 0.45
    rotor = df_turbinas().select("turbine_id", "diametro_rotor_m")
    area = F.lit(math.pi) * F.pow(F.col("diametro_rotor_m") / 2, 2)
    teorica = (F.when(F.col("wind_speed").isNull() | (F.col("wind_speed") < 3) | (F.col("wind_speed") >= 25), 0.0)
                .otherwise(F.least(F.lit(0.5 * RHO * CP / 1000) * area * F.pow("wind_speed", 3), F.col("potencia_nominal_kw").cast("double"))))
    (spark.table(t("silver_telemetria_enriquecida")).join(rotor, "turbine_id")
        .withColumn("potencia_teorica_kw", teorica)
        .withColumn("rendimiento", F.col("power_kw") / F.when(F.col("potencia_teorica_kw") != 0, F.col("potencia_teorica_kw")))
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_telemetria_final")))

    spark.sql(f"""
    CREATE OR REPLACE FUNCTION {t('fn_potencia_teorica')}(v DOUBLE, rotor_m DOUBLE, nominal_kw DOUBLE)
    RETURNS DOUBLE
    COMMENT 'Potencia teórica kW (Betz simplificado, Cp=0.45)'
    RETURN CASE WHEN v < 3 OR v >= 25 OR v IS NULL THEN 0
                ELSE least(0.5 * 1.225 * pi() * pow(rotor_m/2, 2) * pow(v, 3) * 0.45 / 1000, nominal_kw) END""")

    # Dimensión SCD2: carga inicial y el MERGE con 10 turbinas en mantenimiento, como en el lab
    spark.sql("DROP TABLE IF EXISTS dim_turbina_scd2")
    spark.sql("""
    CREATE TABLE dim_turbina_scd2 (
      turbine_id STRING, park_id STRING, modelo STRING, potencia_nominal_kw INT, estado STRING,
      valido_desde TIMESTAMP, valido_hasta TIMESTAMP, es_actual BOOLEAN)""")
    spark.sql("""
    INSERT INTO dim_turbina_scd2
    SELECT turbine_id, park_id, modelo, potencia_nominal_kw, estado, timestamp'2000-01-01', NULL, true
    FROM silver_dim_turbina""")
    (spark.table(t("dim_turbina_scd2")).filter("es_actual").drop("valido_desde", "valido_hasta", "es_actual")
        .withColumn("estado", F.when(F.col("turbine_id").isin([f"WTG-{i:04d}" for i in range(1, 11)]), "MANTENIMIENTO")
                               .otherwise(F.col("estado")))
        .createOrReplaceTempView("extracto_turbinas"))
    spark.sql("""
    MERGE INTO dim_turbina_scd2 AS d
    USING (
      SELECT e.turbine_id AS merge_key, e.* FROM extracto_turbinas e
      UNION ALL
      SELECT NULL AS merge_key, e.* FROM extracto_turbinas e
      JOIN dim_turbina_scd2 d ON e.turbine_id = d.turbine_id AND d.es_actual AND e.estado <> d.estado
    ) AS s
    ON d.turbine_id = s.merge_key AND d.es_actual
    WHEN MATCHED AND d.estado <> s.estado THEN
      UPDATE SET d.es_actual = false, d.valido_hasta = current_timestamp()
    WHEN NOT MATCHED THEN
      INSERT (turbine_id, park_id, modelo, potencia_nominal_kw, estado, valido_desde, valido_hasta, es_actual)
      VALUES (s.turbine_id, s.park_id, s.modelo, s.potencia_nominal_kw, s.estado, current_timestamp(), NULL, true)""")

    spark.sql("""
    CREATE OR REPLACE TABLE gold_kpi_turbina_dia
    COMMENT 'KPIs diarios por aerogenerador: energía, factor de capacidad, disponibilidad y rendimiento'
    AS
    SELECT
      date(ts) AS fecha,
      turbine_id, park_id, parque, modelo, potencia_nominal_kw,
      round(sum(energia_kwh) / 1000, 3) AS energia_mwh,
      round(sum(energia_kwh) / NULLIF(potencia_nominal_kw * 24, 0), 4) AS factor_capacidad,
      round(count_if(wind_speed BETWEEN 3 AND 25 AND power_kw > 0)
            / NULLIF(count_if(wind_speed BETWEEN 3 AND 25), 0), 4) AS disponibilidad,
      round(avg(rendimiento), 3) AS rendimiento_medio,
      round(max(temp_gearbox_c), 1) AS temp_gearbox_max,
      round(avg(vibration_mm_s), 3) AS vibracion_media
    FROM silver_telemetria_final
    GROUP BY ALL""")
    spark.sql("""
    CREATE OR REPLACE TABLE gold_kpi_parque_dia
    COMMENT 'KPIs diarios por parque eólico'
    AS
    SELECT fecha, park_id, parque,
           count(DISTINCT turbine_id) AS turbinas,
           round(sum(energia_mwh), 2) AS energia_mwh,
           round(avg(factor_capacidad), 4) AS factor_capacidad_medio,
           round(avg(disponibilidad), 4) AS disponibilidad_media
    FROM gold_kpi_turbina_dia GROUP BY ALL""")
    for col, txt in [("factor_capacidad", "Energía real / energía máxima teórica a potencia nominal durante 24 h (0-1)"),
                     ("disponibilidad", "Fracción de minutos con viento operable (3-25 m/s) en los que la turbina produjo"),
                     ("rendimiento_medio", "Potencia real / potencia teórica. <0.8 sostenido = revisar")]:
        spark.sql(f"ALTER TABLE gold_kpi_turbina_dia ALTER COLUMN {col} COMMENT '{txt}'")
    print("gold_kpi_turbina_dia:", spark.table(t("gold_kpi_turbina_dia")).count(), "filas")
else:
    print("Gold: no hace falta crear nada")

# COMMAND ----------

print(f"Listo para empezar el día {DIA}.")
