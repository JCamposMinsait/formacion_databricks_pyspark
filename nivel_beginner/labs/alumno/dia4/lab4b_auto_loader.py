# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4b · Auto Loader y checkpoint (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~35 min
# MAGIC
# MAGIC Cargas en Bronze los ficheros que deja el simulador, solo los nuevos cada vez, y ves cómo el checkpoint recuerda lo que ya se ha leído.
# MAGIC
# MAGIC > Necesitas el simulador (`lab4a_simulador`) en marcha en otra pestaña. En la cuenta gratuita solo hay serverless, así que la carga se lanza con `availableNow`: procesa lo pendiente y se para.
# MAGIC
# MAGIC > **Cómo seguir esta guía**
# MAGIC > - Va paso a paso: lee la explicación, ejecuta la celda y mira el resultado. El código de los pasos difíciles ya está escrito.
# MAGIC > - En los ejercicios **Ahora tú** te toca escribir a ti: rellena lo marcado con `___`. Si te atascas, mira el código de justo encima.
# MAGIC > - Los retos del final son para quien vaya rápido.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

CHECKPOINT = f"{RUTA_CHECKPOINT}/bronze_stream"
ESQUEMA_AL = f"{RUTA_SCHEMAS}/bronze_stream"


def filas_bronze():
    return spark.table(t("bronze_telemetria_stream")).count() if spark.catalog.tableExists(t("bronze_telemetria_stream")) else 0


def cargar():
    """Carga con Auto Loader los ficheros nuevos de telemetria_stream y dice cuántas filas han entrado."""
    antes = filas_bronze()
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", ESQUEMA_AL)          # dónde guarda el esquema que ha deducido
        .option("cloudFiles.schemaHints", "ts TIMESTAMP, value DOUBLE")
        .load(RUTA_RAW_STREAM)
        .select("*", F.col("_metadata.file_path").alias("_source_file"))
        .writeStream
        .option("checkpointLocation", CHECKPOINT)                 # la memoria: qué ficheros ya se han leído
        .option("mergeSchema", "true")
        .trigger(availableNow=True)                               # procesa lo pendiente y se para
        .toTable(t("bronze_telemetria_stream"))
        .awaitTermination())
    print(f"Filas nuevas: {filas_bronze() - antes} | total: {filas_bronze()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Auto Loader a Bronze
# MAGIC Lanza la carga. La primera vez lee todos los ficheros que haya en la carpeta.

# COMMAND ----------

cargar()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Espera medio minuto, con el simulador en marcha, y relanza la carga.
# MAGIC
# MAGIC **Comprueba:** solo entran las filas de los ficheros nuevos, no todo otra vez.

# COMMAND ----------

___()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El checkpoint
# MAGIC En la carpeta del checkpoint, el stream apunta qué ha leído (offsets) y qué ha terminado de escribir (commits). Si se borra, vuelve a empezar desde el principio.

# COMMAND ----------

display(dbutils.fs.ls(CHECKPOINT))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta cuántos ficheros distintos se han cargado ya en la Bronze, con `_source_file`.
# MAGIC
# MAGIC **Comprueba:** sale el número de ficheros que ha escrito el simulador hasta la última carga.

# COMMAND ----------

print(spark.table(t("bronze_telemetria_stream")).select(___).distinct().count(), "ficheros cargados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Un campo nuevo
# MAGIC A partir de su iteración 30, unos cinco minutos después de arrancar, el simulador añade el campo `firmware_build`. Auto Loader se para al verlo, apunta la columna y, al relanzarlo, sigue con ella. Ejecuta la primera celda y, si se para, la segunda.

# COMMAND ----------

try:
    cargar()
except Exception as e:
    print("La carga se ha parado por una columna nueva. Es lo esperado: relánzala en la celda siguiente.")

# COMMAND ----------

cargar()
spark.table(t("bronze_telemetria_stream")).printSchema()     # aparece firmware_build

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Consultar lo cargado
# MAGIC La Bronze de streaming es una tabla Delta como cualquier otra: se consulta igual.

# COMMAND ----------

display(spark.table(t("bronze_telemetria_stream")).groupBy("signal").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC La temperatura máxima de la multiplicadora de cada turbina, quedándote con las que pasan de 75 ºC.
# MAGIC
# MAGIC **Comprueba:** tiene que salir WTG-0013, que el simulador va calentando poco a poco.

# COMMAND ----------

display(spark.table(t("bronze_telemetria_stream")).filter(F.col("signal") == ___)
        .groupBy("turbine_id").agg(F.max("value").alias("temp_max")).filter(F.col("temp_max") > ___))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para ir más allá (avanzado)
# MAGIC 1. Una Gold en streaming: temperatura máxima por turbina y minuto con ventanas de un minuto y un watermark de 2 minutos.
# MAGIC 2. Una tabla con la última lectura de cada turbina, actualizada con `foreachBatch` y un MERGE en cada lote.
# MAGIC 3. Repite la carga con el modo `rescue` y un esquema fijo: ¿dónde acaba `firmware_build`?
