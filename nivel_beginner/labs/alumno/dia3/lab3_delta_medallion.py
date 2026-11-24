# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 · Delta Lake y de Bronze a Gold (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~2 h
# MAGIC
# MAGIC Practicas Delta con una tabla pequeña y después haces el recorrido completo: la Bronze del lunes se limpia en Silver y acaba en una Gold con los KPIs de cada parque y día.
# MAGIC
# MAGIC > Lo mínimo que tienes que acabar son las tablas Gold: el jueves y el viernes trabajamos sobre ellas. Si te falta la Bronze, ejecuta antes `00_setup/ponerse_al_dia` eligiendo el día 3.
# MAGIC
# MAGIC > **Cómo seguir esta guía**
# MAGIC > - Va paso a paso: lee la explicación, ejecuta la celda y mira el resultado. El código de los pasos difíciles ya está escrito.
# MAGIC > - En los ejercicios **Ahora tú** te toca escribir a ti: rellena lo marcado con `___`. Si te atascas, mira el código de justo encima.
# MAGIC > - Los retos del final son para quien vaya rápido.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Delta: crear y escribir
# MAGIC Una tabla pequeña para practicar. Cada escritura queda apuntada en el diario de la tabla como una versión nueva.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {t('delta_turbinas')}")
df_turbinas().limit(5).select("turbine_id", "modelo", F.lit("OPERATIVA").alias("estado")) \
    .write.saveAsTable(t("delta_turbinas"))
spark.sql(f"UPDATE {t('delta_turbinas')} SET estado = 'MANTENIMIENTO' WHERE turbine_id = 'WTG-0003'")
spark.sql(f"DELETE FROM {t('delta_turbinas')} WHERE turbine_id = 'WTG-0005'")
display(spark.table(t("delta_turbinas")).orderBy("turbine_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Pon la turbina `WTG-0001` en estado `PARADA` y mira el historial de la tabla.
# MAGIC
# MAGIC **Comprueba:** en el historial salen cuatro versiones: la creación, el UPDATE, el DELETE y tu UPDATE.

# COMMAND ----------

spark.sql(f"UPDATE {t('delta_turbinas')} SET estado = ___ WHERE turbine_id = ___")
display(spark.sql(f"DESCRIBE HISTORY {t('delta_turbinas')}").select("version", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Historial: time travel y RESTORE
# MAGIC Se puede consultar cualquier versión anterior y volver a ella. RESTORE no borra la historia: crea una versión nueva igual a la antigua.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {t('delta_turbinas')} VERSION AS OF 0 ORDER BY turbine_id"))   # recién creada

# COMMAND ----------

spark.sql(f"RESTORE TABLE {t('delta_turbinas')} TO VERSION AS OF 0")
display(spark.table(t("delta_turbinas")).orderBy("turbine_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta las filas de la tabla en la versión 2, la del DELETE.
# MAGIC
# MAGIC **Comprueba:** salen 4 filas: la versión 2 es justo después de borrar WTG-0005.

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS filas FROM {t('delta_turbinas')} VERSION AS OF ___"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · MERGE: el maestro al día
# MAGIC El maestro de turbinas es una tabla de Silver. Llega un extracto del sistema de mantenimiento: diez turbinas han pasado a mantenimiento. MERGE actualiza las que ya existen e inserta las nuevas, y se puede relanzar sin duplicar.

# COMMAND ----------

maestro = (df_turbinas().withColumn("estado", F.lit("OPERATIVA"))
           .select("turbine_id", "park_id", "modelo", "potencia_nominal_kw", "estado")
           .join(df_parques().select("park_id", F.col("nombre").alias("parque"), "provincia", "operador"), "park_id"))
maestro.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_dim_turbina"))

# El extracto: las turbinas 1 a 10 pasan a mantenimiento
extracto = (spark.table(t("silver_dim_turbina"))
            .filter(F.col("turbine_id").isin([f"WTG-{i:04d}" for i in range(1, 11)]))
            .withColumn("estado", F.lit("MANTENIMIENTO")))
extracto.createOrReplaceTempView("extracto_turbinas")
MERGE = f"""
MERGE INTO {t('silver_dim_turbina')} d
USING extracto_turbinas e ON d.turbine_id = e.turbine_id
WHEN MATCHED AND d.estado <> e.estado THEN UPDATE SET d.estado = e.estado
WHEN NOT MATCHED THEN INSERT *"""
display(spark.sql(MERGE))     # 10 filas actualizadas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Relanza el MERGE y cuenta cuántas turbinas hay en cada estado.
# MAGIC
# MAGIC **Comprueba:** el MERGE relanzado no actualiza nada (num_updated_rows = 0) y salen 10 en MANTENIMIENTO y 230 OPERATIVAS.

# COMMAND ----------

display(spark.sql(___))
display(spark.table(t("silver_dim_turbina")).groupBy(___).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Silver: limpiar y deduplicar
# MAGIC En Silver el dato queda limpio: `ts` como fecha y hora, el `-9999` convertido en nulo, las potencias negativas a cero y sin lecturas repetidas.

# COMMAND ----------

bronze = spark.table(t("bronze_telemetria"))
limpio = (bronze
          .withColumn("ts", F.to_timestamp("ts"))
          .withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
          .withColumn("value", F.when((F.col("signal") == "power_kw") & (F.col("value") < 0), 0.0).otherwise(F.col("value")))
          .dropDuplicates(["turbine_id", "ts", "signal"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC ¿Cuántas lecturas repetidas se han quitado?
# MAGIC
# MAGIC **Comprueba:** sale alrededor del 0,5 % de las lecturas de la Bronze.

# COMMAND ----------

print("Repetidas quitadas:", bronze.count() - ___.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Silver: cruzar con el maestro
# MAGIC Cada lectura se enriquece con los datos de su turbina. Las lecturas de turbinas que no están en el maestro no se tiran: van a una tabla de cuarentena para revisarlas.

# COMMAND ----------

dim = spark.table(t("silver_dim_turbina"))
silver = limpio.join(F.broadcast(dim.drop("park_id")), "turbine_id", "left")
(silver.filter(F.col("modelo").isNotNull()).drop("_source_file")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_telemetria")))
(silver.filter(F.col("modelo").isNull())
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_cuarentena")))
print("silver_telemetria:", spark.table(t("silver_telemetria")).count(), "filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta las lecturas en cuarentena por turbina.
# MAGIC
# MAGIC **Comprueba:** salen WTG-0901 y WTG-0902, las dos turbinas que no existen en el maestro.

# COMMAND ----------

display(spark.table(t(___)).groupBy("turbine_id").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Gold: KPIs por parque y día
# MAGIC Primero pasamos de una fila por señal a una columna por señal, que es como se analiza cómodo. Después calculamos la potencia teórica de cada minuto, la que daría el viento según el tamaño del rotor, para medir el rendimiento. Estos dos pasos van hechos.

# COMMAND ----------

import math

spark.sql(f"""
CREATE OR REPLACE TABLE {t('silver_telemetria_wide')} AS
SELECT * FROM (
  SELECT turbine_id, park_id, parque, modelo, potencia_nominal_kw, ts, signal, value
  FROM {t('silver_telemetria')} WHERE quality = 'GOOD'
)
PIVOT (avg(value) FOR signal IN ('wind_speed' AS wind_speed, 'power_kw' AS power_kw, 'rotor_rpm' AS rotor_rpm,
                                 'temp_gearbox_c' AS temp_gearbox_c, 'vibration_mm_s' AS vibration_mm_s))""")

RHO, CP = 1.225, 0.45
area = F.lit(math.pi) * F.pow(F.col("diametro_rotor_m") / 2, 2)
teorica = (F.when(F.col("wind_speed").isNull() | (F.col("wind_speed") < 3) | (F.col("wind_speed") >= 25), 0.0)
            .otherwise(F.least(F.lit(0.5 * RHO * CP / 1000) * area * F.pow("wind_speed", 3), F.col("potencia_nominal_kw").cast("double"))))
(spark.table(t("silver_telemetria_wide"))
    .join(df_turbinas().select("turbine_id", "diametro_rotor_m"), "turbine_id")
    .withColumn("potencia_teorica_kw", teorica)
    .withColumn("rendimiento", F.col("power_kw") / F.when(F.col("potencia_teorica_kw") != 0, F.col("potencia_teorica_kw")))
    .withColumn("energia_kwh", F.coalesce("power_kw", F.lit(0.0)) / 60)      # una lectura por minuto
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_telemetria_final")))
display(spark.table(t("silver_telemetria_final")).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Con eso ya se calculan los KPIs de cada turbina y día. La energía es la suma de la de cada minuto, el factor de capacidad compara lo producido con lo que habría dado la turbina a plena potencia todo el día y la disponibilidad mide cuánto produjo cuando había viento.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {t('gold_kpi_turbina_dia')}
COMMENT 'KPIs diarios por aerogenerador: energía, factor de capacidad, disponibilidad y rendimiento'
AS
SELECT
  date(ts) AS fecha,
  turbine_id, park_id, parque, modelo, potencia_nominal_kw,
  round(sum(energia_kwh) / 1000, 3) AS energia_mwh,
  round(sum(energia_kwh) / NULLIF(potencia_nominal_kw * 24, 0), 4) AS factor_capacidad,
  round(count_if(wind_speed BETWEEN 3 AND 25 AND power_kw > 0) / NULLIF(count_if(wind_speed BETWEEN 3 AND 25), 0), 4) AS disponibilidad,
  round(avg(rendimiento), 3) AS rendimiento_medio,
  round(max(temp_gearbox_c), 1) AS temp_gearbox_max,
  round(avg(vibration_mm_s), 3) AS vibracion_media
FROM {t('silver_telemetria_final')}
GROUP BY ALL""")
display(spark.table(t("gold_kpi_turbina_dia")).orderBy("fecha", "turbine_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Crea `gold_kpi_parque_dia` a partir de la Gold por turbina: por fecha y parque, el número de turbinas, la energía total y la media del factor de capacidad y de la disponibilidad.
# MAGIC
# MAGIC **Comprueba:** sale una fila por parque y día. P01 es el que más energía produce, porque tiene más turbinas.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {t('gold_kpi_parque_dia')}
COMMENT 'KPIs diarios por parque eólico'
AS
SELECT fecha, park_id, parque,
       count(DISTINCT ___) AS turbinas,
       round(sum(___), 2) AS energia_mwh,
       round(avg(factor_capacidad), 4) AS factor_capacidad_medio,
       round(avg(___), 4) AS disponibilidad_media
FROM {t('gold_kpi_turbina_dia')} GROUP BY ALL""")
display(spark.table(t("gold_kpi_parque_dia")).orderBy("fecha", "park_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Documentar
# MAGIC Los comentarios dicen qué es cada tabla y columna y cómo se calcula. Los lee quien consulta la tabla y, el viernes, también Genie.

# COMMAND ----------

for col, txt in [("factor_capacidad", "Energía real / energía máxima a potencia nominal durante 24 h (0-1)"),
                 ("disponibilidad", "Fracción de minutos con viento operable (3-25 m/s) en los que la turbina produjo"),
                 ("rendimiento_medio", "Potencia real / potencia teórica. Por debajo de 0,8 de forma sostenida hay que revisarla")]:
    spark.sql(f"ALTER TABLE {t('gold_kpi_turbina_dia')} ALTER COLUMN {col} COMMENT '{txt}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Pon un comentario a la columna `energia_mwh` de `gold_kpi_parque_dia` y míralo en Catalog Explorer.
# MAGIC
# MAGIC **Comprueba:** en Catalog Explorer, en la pestaña Overview de la tabla, la columna lleva tu comentario.

# COMMAND ----------

spark.sql(f"ALTER TABLE {t('gold_kpi_parque_dia')} ALTER COLUMN ___ COMMENT ___")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para ir más allá (avanzado)
# MAGIC 1. Con `LAG`, calcula para cada lectura los minutos desde la anterior de la misma turbina y cuenta los huecos de más de 5 minutos por parque.
# MAGIC 2. Guarda la historia del maestro como SCD tipo 2 en `dim_turbina_scd2`, con `valido_desde`, `valido_hasta` y `es_actual`, usando un MERGE.
# MAGIC 3. Las 10 turbinas con peor rendimiento medio y vibración por encima del percentil 90.
