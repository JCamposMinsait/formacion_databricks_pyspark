# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4b · Auto Loader y checkpoint
# MAGIC
# MAGIC Duración: 35 min aproximadamente.
# MAGIC
# MAGIC Vamos a cargar en Bronze los ficheros que deja el simulador, solo los nuevos en cada pasada, y a ver cómo el checkpoint recuerda lo que ya se ha leído.
# MAGIC
# MAGIC > Necesitas el simulador (`lab4a_simulador`) en marcha en otra pestaña. En la cuenta gratuita solo hay serverless, así que la carga se lanza con `availableNow`, que procesa lo pendiente y se para.
# MAGIC
# MAGIC En las celdas de **Ahora tú** hay que completar lo que está marcado con `___`. En la última escribes tú el código entero. Los retos del final son opcionales y van de menos a más.

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
# MAGIC Solo deberían entrar las filas de los ficheros nuevos.

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
# MAGIC Debería salir el número de ficheros que había escrito el simulador en la última carga.

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
# MAGIC La Bronze de streaming es una tabla Delta normal y se consulta igual que las demás.

# COMMAND ----------

display(spark.table(t("bronze_telemetria_stream")).groupBy("signal").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC La temperatura máxima de la multiplicadora de cada turbina, quedándote con las que pasan de 75 ºC. Esta vez escribes tú la celda entera.
# MAGIC
# MAGIC Tiene que salir WTG-0013, que el simulador va calentando poco a poco.

# COMMAND ----------

# Filtra temp_gearbox_c, agrupa por turbina, saca el máximo y quédate con las que pasan de 75


# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC Van de menos a más. El último es para quien vaya sobrado.
# MAGIC
# MAGIC 1. ¿Cuántas filas trae cada fichero cargado? Agrupa por `_source_file`. Todos deberían traer 1.200 lecturas, 240 turbinas por 5 señales.
# MAGIC 2. Carga los mismos ficheros en otra tabla, `reto_bronze_copia`, con un checkpoint y una ruta de esquema nuevos. Copia la función `cargar` y cambia esas tres cosas. ¿Por qué entra todo de golpe?
# MAGIC 3. Repite la carga con un esquema fijo y el modo `rescue` (`cloudFiles.schemaEvolutionMode`). ¿Dónde acaba `firmware_build`?
# MAGIC 4. Una tabla con la última lectura de temperatura de cada turbina, actualizada con `foreachBatch` y un MERGE en cada lote. Dentro de la función, la tabla se abre con `DeltaTable.forName(lote.sparkSession, tabla)`. En serverless hay que usar la sesión del lote, no `spark`.
# MAGIC 5. Una Gold en streaming con la temperatura máxima por turbina y minuto, con `F.window("ts", "1 minute")`, un watermark de 2 minutos (`withWatermark`) y `outputMode("append")`. Solo salen los minutos que el watermark ya ha cerrado. ¿Por qué?
