# Databricks notebook source
# MAGIC %md
# MAGIC # Día 3: Delta Lake y arquitectura medallion

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Una tabla Delta

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE {t('demo_delta')} (turbine_id STRING, estado STRING, potencia_kw INT)")
spark.sql(f"INSERT INTO {t('demo_delta')} VALUES ('WTG-0001', 'OPERATIVA', 4200), ('WTG-0002', 'OPERATIVA', 4500), ('WTG-0003', 'PARADA', 5000)")
display(spark.sql(f"DESCRIBE DETAIL {t('demo_delta')}").select("format", "numFiles", "sizeInBytes", "location"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cambios e historial

# COMMAND ----------

spark.sql(f"UPDATE {t('demo_delta')} SET estado = 'MANTENIMIENTO' WHERE turbine_id = 'WTG-0003'")
spark.sql(f"DELETE FROM {t('demo_delta')} WHERE turbine_id = 'WTG-0002'")
spark.sql(f"INSERT INTO {t('demo_delta')} VALUES ('WTG-0004', 'OPERATIVA', 4200)")
display(spark.table(t("demo_delta")).orderBy("turbine_id"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {t('demo_delta')}").select("version", "timestamp", "operation", "operationMetrics").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time travel y RESTORE

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {t('demo_delta')} VERSION AS OF 1 ORDER BY turbine_id"))   # como estaba tras el primer INSERT

# COMMAND ----------

spark.sql(f"RESTORE TABLE {t('demo_delta')} TO VERSION AS OF 1")
display(spark.table(t("demo_delta")).orderBy("turbine_id"))
display(spark.sql(f"DESCRIBE HISTORY {t('demo_delta')}").select("version", "operation").orderBy("version"))   # el RESTORE es una versión más

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {t('demo_maestro')} AS
    SELECT * FROM VALUES ('WTG-0001', 'OPERATIVA'), ('WTG-0002', 'OPERATIVA'), ('WTG-0003', 'OPERATIVA') AS m(turbine_id, estado)""")
cambios = spark.createDataFrame([("WTG-0002", "MANTENIMIENTO"), ("WTG-0004", "OPERATIVA")], "turbine_id string, estado string")
cambios.createOrReplaceTempView("cambios_v")
MERGE = f"""
MERGE INTO {t('demo_maestro')} m
USING cambios_v c ON m.turbine_id = c.turbine_id
WHEN MATCHED AND m.estado <> c.estado THEN UPDATE SET m.estado = c.estado
WHEN NOT MATCHED THEN INSERT *"""
display(spark.sql(MERGE))                                  # una fila actualizada y otra insertada
display(spark.table(t("demo_maestro")).orderBy("turbine_id"))

# COMMAND ----------

display(spark.sql(MERGE))                                  # relanzado: no cambia nada

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Esquema

# COMMAND ----------

nueva = spark.createDataFrame([("WTG-0005", "OPERATIVA", "2026.11")], "turbine_id string, estado string, firmware string")
try:
    nueva.write.mode("append").saveAsTable(t("demo_maestro"))
except Exception as e:
    print("Rechazada por el esquema:", str(e).splitlines()[0][:160])

# COMMAND ----------

nueva.write.mode("append").option("mergeSchema", "true").saveAsTable(t("demo_maestro"))
display(spark.table(t("demo_maestro")).orderBy("turbine_id"))   # la columna nueva sale con nulos en las filas viejas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ficheros pequeños

# COMMAND ----------

for i in range(20):
    spark.range(i * 100, i * 100 + 100).write.mode("append").saveAsTable(t("demo_small"))
print("ficheros:", spark.sql(f"DESCRIBE DETAIL {t('demo_small')}").first().numFiles)

# COMMAND ----------

spark.sql(f"OPTIMIZE {t('demo_small')}")
print("tras OPTIMIZE:", spark.sql(f"DESCRIBE DETAIL {t('demo_small')}").first().numFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Un medallion en pequeño

# COMMAND ----------

bronze = spark.createDataFrame([
    ("WTG-0004", "2026-11-02 10:00:00", "power_kw", "3200"),
    ("WTG-0004", "2026-11-02 10:00:00", "power_kw", "3200"),     # duplicada
    ("WTG-0008", "2026-11-02 10:00:00", "power_kw", "-9999"),    # sensor sin lectura
    ("WTG-0012", "2026-11-02 10:00:00", "power_kw", "4100"),
    ("WTG-0004", "2026-11-02 10:01:00", "power_kw", "3300")], "turbine_id string, ts string, signal string, value string")
bronze.write.mode("overwrite").saveAsTable(t("demo_bronze"))      # Bronze: tal cual llega
display(spark.table(t("demo_bronze")))

# COMMAND ----------

silver = (spark.table(t("demo_bronze"))
          .dropDuplicates()
          .withColumn("ts", F.to_timestamp("ts"))
          .withColumn("value", F.expr("try_cast(value AS double)"))
          .withColumn("value", F.when(F.col("value") != -9999, F.col("value"))))
silver.write.mode("overwrite").saveAsTable(t("demo_silver"))      # Silver: sin duplicados, tipada y sin centinelas
display(spark.table(t("demo_silver")))

# COMMAND ----------

gold = (spark.table(t("demo_silver"))
        .join(df_turbinas().select("turbine_id", "park_id"), "turbine_id")
        .groupBy("park_id", F.to_date("ts").alias("fecha"))
        .agg(F.round(F.sum("value") / 60 / 1000, 4).alias("energia_mwh")))
gold.write.mode("overwrite").saveAsTable(t("demo_gold"))          # Gold: el KPI que consume el negocio
spark.sql(f"COMMENT ON TABLE {t('demo_gold')} IS 'Energía producida por parque y día'")
spark.sql(f"ALTER TABLE {t('demo_gold')} ALTER COLUMN energia_mwh COMMENT 'Energía en MWh: suma de la potencia de cada minuto'")
display(spark.table(t("demo_gold")))
