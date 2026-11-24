# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1b · La telemetría y la tabla Bronze (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~1 h
# MAGIC
# MAGIC Generas la telemetría de la semana, la exploras sin cargarla y la guardas en la tabla **Bronze**, de la que parte todo lo demás.
# MAGIC
# MAGIC > Lo mínimo que tienes que acabar hoy es la tabla `bronze_telemetria`: mañana se parte de ella.
# MAGIC
# MAGIC > **Cómo seguir esta guía**
# MAGIC > - Va paso a paso: lee la explicación, ejecuta la celda y mira el resultado. El código de los pasos difíciles ya está escrito.
# MAGIC > - En los ejercicios **Ahora tú** te toca escribir a ti: rellena lo marcado con `___`. Si te atascas, mira el código de justo encima.
# MAGIC > - Los retos del final son para quien vaya rápido.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Generar la telemetría
# MAGIC Esta celda hace de concentrador de los parques: escribe ficheros JSON con una lectura por turbina, minuto y señal (formato EAV). Vienen sucios a propósito: valores `-9999` cuando el sensor no tiene lectura, nulos, duplicados, lecturas de calidad `BAD` y turbinas que no existen en el maestro.
# MAGIC
# MAGIC Con `dias = 2` salen unos 3,5 millones de filas. No lo subas: la cuenta gratuita tiene una cuota diaria de cómputo.

# COMMAND ----------

import math, datetime as dt

dbutils.widgets.text("dias", "2")
DIAS = int(dbutils.widgets.get("dias"))
INICIO = dt.datetime(2026, 11, 2, 0, 0, 0, tzinfo=dt.timezone.utc)
DEGRADADAS = ["WTG-0042", "WTG-0117", "WTG-0199"]     # rinden menos de lo que deberían
CALIENTES = ["WTG-0013", "WTG-0187"]                  # la multiplicadora se calienta

base = (df_turbinas().select("turbine_id", "park_id", "potencia_nominal_kw", "diametro_rotor_m")
        .crossJoin(spark.range(0, DIAS * 1440).withColumnRenamed("id", "m"))
        .withColumn("ts", F.timestamp_seconds(F.lit(int(INICIO.timestamp())) + F.col("m") * 60))
        .withColumn("park_bias", F.when(F.col("park_id") == "P03", 1.8).when(F.col("park_id") == "P06", 1.2)
                                  .when(F.col("park_id") == "P02", -1.0).otherwise(0.0))
        .withColumn("wind_speed", F.greatest(F.lit(0.0), F.lit(5.8) + F.col("park_bias")
                     + F.lit(3.0) * F.sin(F.lit(2 * math.pi) * (F.col("m") % 1440) / 1440) + F.randn(seed=1) * 1.8))
        .withColumn("cp", F.when(F.col("turbine_id").isin(DEGRADADAS), 0.30).otherwise(0.40))
        .withColumn("power_kw", F.when((F.col("wind_speed") < 3) | (F.col("wind_speed") >= 25), F.lit(0.0))
                                 .otherwise(F.least(F.col("potencia_nominal_kw").cast("double"),
                                                    F.lit(0.5 * 1.225 * math.pi / 1000) * F.pow(F.col("diametro_rotor_m") / 2, 2)
                                                    * F.pow("wind_speed", 3) * F.col("cp"))) + F.randn(seed=2) * 40)
        .withColumn("rotor_rpm", F.least(F.lit(14.5), F.col("wind_speed") * 1.15) + F.randn(seed=3) * 0.2)
        .withColumn("temp_gearbox_c", F.lit(42.0) + F.col("power_kw") / F.col("potencia_nominal_kw") * 28 + F.randn(seed=4) * 1.5
                    + F.when(F.col("turbine_id").isin(CALIENTES), 8.0).otherwise(0.0))
        .withColumn("vibration_mm_s", F.abs(F.randn(seed=5)) * 0.6 + F.col("rotor_rpm") * 0.05))
eav = (base.selectExpr("turbine_id", "park_id", "ts",
                       "stack(5, 'wind_speed', wind_speed, 'power_kw', power_kw, 'rotor_rpm', rotor_rpm, "
                       "'temp_gearbox_c', temp_gearbox_c, 'vibration_mm_s', vibration_mm_s) as (signal, value)")
       .withColumn("value", F.when(F.rand(seed=6) < 0.002, F.lit(-9999.0))
                             .when(F.rand(seed=7) < 0.003, F.lit(None).cast("double")).otherwise(F.round("value", 3)))
       .withColumn("quality", F.when(F.rand(seed=8) < 0.01, "BAD").otherwise("GOOD"))
       .withColumn("fw_version", F.when(F.col("park_id").isin("P01", "P02"), "3.2.1").otherwise("3.1.0"))
       .withColumn("ts", F.date_format("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
       .withColumn("fecha", F.substring("ts", 1, 10)))
duplicados = eav.sample(fraction=0.005, seed=9)
huerfanas = (eav.filter(F.col("turbine_id").isin("WTG-0001", "WTG-0002"))
             .withColumn("turbine_id", F.when(F.col("turbine_id") == "WTG-0001", "WTG-0901").otherwise("WTG-0902")))
(eav.unionByName(duplicados).unionByName(huerfanas)
    .repartition(48).write.mode("overwrite").partitionBy("fecha").json(RUTA_RAW_BATCH))
print("Telemetría escrita en", RUTA_RAW_BATCH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Explorar los ficheros sin cargarlos
# MAGIC Antes de crear una tabla conviene mirar qué ha llegado. `read_files` deja consultar ficheros con SQL directamente.

# COMMAND ----------

display(dbutils.fs.ls(RUTA_RAW_BATCH))     # una carpeta por día
display(spark.sql(f"SELECT * FROM read_files('{RUTA_RAW_BATCH}', format => 'json') LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta cuántas lecturas hay de cada señal directamente sobre los ficheros.
# MAGIC
# MAGIC **Comprueba:** salen las cinco señales con cifras muy parecidas, alrededor de 700.000 cada una.

# COMMAND ----------

display(spark.sql(f"SELECT signal, count(*) AS lecturas FROM read_files('{RUTA_RAW_BATCH}', format => 'json') GROUP BY ___"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · La tabla Bronze
# MAGIC En Bronze se guarda el dato tal cual llega, sin limpiar, más dos columnas de auditoría: de qué fichero viene cada fila y cuándo se cargó. El esquema se da escrito, que es lo correcto en producción.

# COMMAND ----------

esquema_raw = "turbine_id STRING, park_id STRING, ts STRING, signal STRING, value DOUBLE, quality STRING, fw_version STRING"
bronze = (spark.read.schema(esquema_raw).json(RUTA_RAW_BATCH)
          .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at")))
spark.sql(f"DROP TABLE IF EXISTS {t('bronze_telemetria')}")
bronze.write.mode("overwrite").saveAsTable(t("bronze_telemetria"))
print("bronze_telemetria:", spark.table(t("bronze_telemetria")).count(), "filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta las lecturas de la Bronze por parque, de más a menos.
# MAGIC
# MAGIC **Comprueba:** P01 sale el primero, con muchas más lecturas que el resto.

# COMMAND ----------

display(spark.table(t("bronze_telemetria")).groupBy(___).count().orderBy(F.desc(___)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Los datos sucios
# MAGIC La Bronze no se limpia: se guarda tal cual. Pero conviene saber qué hay, porque el miércoles lo limpiaremos en Silver.

# COMMAND ----------

b = spark.table(t("bronze_telemetria"))
print("Lecturas con -9999:", b.filter(F.col("value") == -9999).count())
print("Lecturas nulas:    ", b.filter(F.col("value").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta cuántas lecturas tienen calidad `BAD`.
# MAGIC
# MAGIC **Comprueba:** sale alrededor del 1 % de las filas.

# COMMAND ----------

print("Calidad BAD:", b.filter(F.col("quality") == ___).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para ir más allá (avanzado)
# MAGIC 1. Cuenta cuántas lecturas están repetidas: misma turbina, instante y señal.
# MAGIC 2. Busca las turbinas que no están en el maestro (`df_turbinas()`) con un `left_anti`.
# MAGIC 3. Mira el historial de la Bronze con `DESCRIBE HISTORY` y la pestaña History en Catalog Explorer.
