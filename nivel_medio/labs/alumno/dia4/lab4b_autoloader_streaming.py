# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4b · Auto Loader y Structured Streaming (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~35 min · Con el simulador (lab 4a) corriendo en otra pestaña.
# MAGIC
# MAGIC > En Free Edition solo hay serverless, que solo admite `trigger(availableNow=True)`. El modo continuo con `processingTime` se ve en la teoría.
# MAGIC
# MAGIC > **Cómo usar esta guía**
# MAGIC > - Cada paso tiene un objetivo, la slide del curso donde está explicado y unas pistas.
# MAGIC > - Rellena lo marcado con `___` o `# TODO`. Si una celda no tiene nada que rellenar, léela y ejecútala.
# MAGIC > - Si te atascas: primero la slide, luego la documentación de Databricks y, como último recurso, la solución en `labs/soluciones/`.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

SERVERLESS = True   # en Free Edition siempre: solo se puede usar availableNow
def trigger_opts(w):
    return w.trigger(availableNow=True) if SERVERLESS else w.trigger(processingTime="10 seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · COPY INTO
# MAGIC **Objetivo:** cargar los ficheros del landing de forma idempotente con SQL.
# MAGIC
# MAGIC **Slide de referencia:** Día 4 · Cuatro formas de llevar ficheros a una tabla · Auto Loader
# MAGIC
# MAGIC **Pistas**
# MAGIC - Crea una tabla vacía con CREATE TABLE IF NOT EXISTS y luego COPY INTO con FILEFORMAT = JSON.
# MAGIC - Ejecútalo dos veces: ¿cuántas filas inserta la segunda?

# COMMAND ----------

spark.sql(f"CREATE TABLE IF NOT EXISTS {t('bronze_copy_into')}")
# TODO
display(spark.sql(f"""
COPY INTO {t('bronze_copy_into')}
FROM '{RUTA_RAW_STREAM}'
FILEFORMAT = ___
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Auto Loader a Bronze
# MAGIC **Objetivo:** ingerir en streaming con evolución de esquema.
# MAGIC
# MAGIC **Slide de referencia:** Día 4 · Auto Loader · Checkpoint: la memoria del stream · Triggers
# MAGIC
# MAGIC **Pistas**
# MAGIC - Formato cloudFiles, con cloudFiles.format = json y cloudFiles.schemaLocation.
# MAGIC - schemaEvolutionMode = addNewColumns: en la iteración 30 del simulador aparece una columna nueva y el stream se parará. Relánzalo.
# MAGIC - Cada stream necesita su propio checkpointLocation.

# COMMAND ----------

bronze_stream = (spark.readStream.format(___)                                     # TODO
    .option(___, "json")                                                            # TODO
    .option(___, f"{RUTA_SCHEMAS}/bronze_stream")                                   # TODO
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaHints", "value DOUBLE, ts TIMESTAMP")
    .load(RUTA_RAW_STREAM)
    .select("*", F.col("_metadata.file_path").alias("_source_file"), F.current_timestamp().alias("_ingested_at")))

q_bronze = trigger_opts(bronze_stream.writeStream
    .option("checkpointLocation", ___)                                              # TODO
    .option("mergeSchema", "true")
    .queryName("bronze_stream")).toTable(t("bronze_telemetria_stream"))
if SERVERLESS:
    q_bronze.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS filas, count(DISTINCT _source_file) AS ficheros, max(ts) AS ultimo_ts FROM bronze_telemetria_stream;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experimento del checkpoint
# MAGIC 1. Para el stream y vuelve a lanzar la celda de Auto Loader.
# MAGIC 2. ¿Ha duplicado filas? ¿Por qué no?
# MAGIC 3. Mira la carpeta del checkpoint: `display(dbutils.fs.ls(f"{RUTA_CHECKPOINT}/bronze_stream"))`. ¿Qué hay en offsets, commits y sources?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Ventanas con watermark → Gold casi en tiempo real
# MAGIC **Objetivo:** potencia media y temperatura máxima por turbina y minuto.
# MAGIC
# MAGIC **Slide de referencia:** Día 4 · Watermarks · Output modes
# MAGIC
# MAGIC **Pistas**
# MAGIC - withWatermark("ts", "2 minutes") antes del groupBy.
# MAGIC - Ventanas de 1 minuto con F.window("ts", "1 minute").
# MAGIC - Output mode append: cada ventana se escribe una vez, cuando se cierra.

# COMMAND ----------

wide = (spark.readStream.table(t("bronze_telemetria_stream"))
        .withWatermark(___, ___)                                                    # TODO
        .groupBy(___, "turbine_id", "park_id")                                      # TODO: ventana de 1 minuto
        .agg(F.avg(F.when(F.col("signal") == "power_kw", F.col("value"))).alias("power_kw_avg"),
             F.max(F.when(F.col("signal") == "temp_gearbox_c", F.col("value"))).alias("temp_gearbox_max"),
             F.count("*").alias("lecturas"))
        .select(F.col("window.start").alias("minuto"), "turbine_id", "park_id", "power_kw_avg", "temp_gearbox_max", "lecturas"))

q_gold = trigger_opts(wide.writeStream
    .outputMode(___)                                                                # TODO
    .option("checkpointLocation", f"{RUTA_CHECKPOINT}/gold_minuto")
    .queryName("gold_minuto")).toTable(t("gold_turbina_minuto_rt"))
if SERVERLESS:
    q_gold.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: turbinas con la multiplicadora por encima de 75 ºC, de la más reciente a la más antigua
# MAGIC SELECT minuto, turbine_id, park_id, round(temp_gearbox_max, 1) AS temp_max
# MAGIC FROM gold_turbina_minuto_rt
# MAGIC WHERE ___
# MAGIC ORDER BY minuto DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. Pon el watermark a 10 segundos y cuenta cuántas lecturas tardías se pierden.
# MAGIC 2. Escribe la Gold con `foreachBatch` y un `MERGE` en vez de append. Día 4 · foreachBatch
# MAGIC 3. Cambia `schemaEvolutionMode` a `rescue`: ¿dónde acaba `firmware_build`?
# MAGIC 4. (Examen) Directory listing o file notification con 5 millones de ficheros al día.
# MAGIC 5. Al acabar: `for q in spark.streams.active: q.stop()` y para el simulador.
