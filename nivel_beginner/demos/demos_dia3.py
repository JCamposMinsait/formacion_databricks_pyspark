# Databricks notebook source
# MAGIC %md
# MAGIC # Día 3: Delta Lake y arquitectura medallion
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="3"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Una tabla Delta
# MAGIC Una tabla Delta son ficheros Parquet más un registro de cambios (`_delta_log`). Cada operación que la cambia queda apuntada como una versión nueva. Por eso se puede hacer `UPDATE`, `DELETE` y `MERGE`, ver el historial y volver atrás. En Databricks, cualquier `CREATE TABLE` crea una tabla Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crea la tabla demo_delta con turbine_id STRING, estado STRING y potencia_kw INT.
# MAGIC -- Mete tres turbinas: WTG-0001 OPERATIVA 4200, WTG-0002 OPERATIVA 4500 y WTG-0003 PARADA 5000.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mira los detalles de demo_delta con DESCRIBE DETAIL: formato, número de ficheros y tamaño.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cambios e historial

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tres cambios, cada uno en su sentencia:
# MAGIC -- WTG-0003 pasa a MANTENIMIENTO, WTG-0002 se borra y entra WTG-0004 OPERATIVA 4200.
# MAGIC -- Después enseña la tabla ordenada por turbine_id.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El historial de demo_delta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time travel y RESTORE
# MAGIC Como cada versión queda guardada, se puede consultar la tabla tal y como estaba en una versión (`VERSION AS OF`) o en una hora (`TIMESTAMP AS OF`), y devolverla a ese punto con `RESTORE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- demo_delta tal y como estaba en la versión 1, la del primer INSERT.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Devuelve demo_delta a la versión 1 y enséñala.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo_delta   -- el RESTORE es una versión más

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE
# MAGIC `MERGE` cruza una tabla con unos cambios por una clave y, en una sola operación, actualiza las filas que ya existen e inserta las nuevas. Es la forma normal de tener al día un maestro, y se puede relanzar sin duplicar nada.

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {t('demo_maestro')} AS
    SELECT * FROM VALUES ('WTG-0001', 'OPERATIVA'), ('WTG-0002', 'OPERATIVA'), ('WTG-0003', 'OPERATIVA') AS m(turbine_id, estado)""")
cambios = spark.createDataFrame([("WTG-0002", "MANTENIMIENTO"), ("WTG-0004", "OPERATIVA")], "turbine_id string, estado string")
cambios.createOrReplaceTempView("cambios_v")
display(spark.table(t("demo_maestro")).orderBy("turbine_id"))
display(cambios)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE de cambios_v en demo_maestro por turbine_id:
# MAGIC -- si la turbina existe y su estado es distinto, actualiza el estado; si no existe, insértala.
# MAGIC -- Ejecuta la celda dos veces y compara la salida.

# COMMAND ----------

display(spark.table(t("demo_maestro")).orderBy("turbine_id"))   # cómo ha quedado el maestro

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Esquema
# MAGIC Delta guarda el esquema de la tabla y rechaza escrituras que no encajan. Así una columna nueva o un tipo cambiado en el origen no ensucian la tabla sin que nadie se entere. Si el cambio es buscado, se permite de forma explícita.

# COMMAND ----------

nueva = spark.createDataFrame([("WTG-0005", "OPERATIVA", "2026.11")], "turbine_id string, estado string, firmware string")
try:
    nueva.write.mode("append").saveAsTable(t("demo_maestro"))
except Exception as e:
    print("Rechazada por el esquema:", str(e).splitlines()[0][:160])

# COMMAND ----------

# Repite la escritura de nueva en demo_maestro, esta vez con la opción mergeSchema, y enseña la tabla.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ficheros pequeños
# MAGIC Cada escritura deja al menos un fichero. Muchas escrituras pequeñas dejan muchos ficheros diminutos, y leerlos es lento porque se pierde más tiempo abriéndolos que leyendo. `OPTIMIZE` los junta en pocos ficheros grandes.

# COMMAND ----------

# 20 escrituras de 100 filas cada una
for i in range(20):
    spark.range(i * 100, i * 100 + 100).write.mode("append").saveAsTable(t("demo_small"))
print("ficheros:", spark.sql(f"DESCRIBE DETAIL {t('demo_small')}").first().numFiles)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compacta demo_small.

# COMMAND ----------

print("tras OPTIMIZE:", spark.sql(f"DESCRIBE DETAIL {t('demo_small')}").first().numFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Un medallion en pequeño
# MAGIC - **Bronze**: los datos tal cual llegan, con sus duplicados y sus errores. Si algo sale mal después, se puede rehacer todo desde aquí.
# MAGIC - **Silver**: limpios, sin duplicados y con los tipos correctos. Es la capa en la que se puede confiar.
# MAGIC - **Gold**: agregados y listos para un informe o un cuadro de mando.

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

# Silver a partir de demo_bronze:
# 1. Quita las filas duplicadas.
# 2. ts a timestamp.
# 3. value a double con try_cast.
# 4. El -9999 a nulo.
# Guárdala como demo_silver y enséñala.

# COMMAND ----------

# Gold a partir de demo_silver: energía en MWh por parque y día.
# 1. Une con df_turbinas() por turbine_id para tener park_id.
# 2. Agrupa por park_id y por la fecha de ts (F.to_date).
# 3. Energía = suma de value / 60 / 1000, redondeada a 4 decimales.
# Guárdala como demo_gold y enséñala.

# COMMAND ----------

# MAGIC %md
# MAGIC La Gold la va a usar gente que no ha visto este código. Un comentario en la tabla y en cada columna dice qué es cada cosa, y sale en Catalog y en las búsquedas.

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {t('demo_gold')} IS 'Energía producida por parque y día'")
spark.sql(f"ALTER TABLE {t('demo_gold')} ALTER COLUMN energia_mwh COMMENT 'Energía en MWh: suma de la potencia de cada minuto'")
display(spark.sql(f"DESCRIBE TABLE {t('demo_gold')}"))
