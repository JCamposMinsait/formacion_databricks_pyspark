# Databricks notebook source
# MAGIC %md
# MAGIC # Día 4: Streaming, pipelines y Lakeflow Jobs

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="4"

# COMMAND ----------

import json, datetime as dt

DEMO_IN = f"{RUTA_VOL}/demo_stream_in"
DEMO_CK = f"{RUTA_VOL}/_demo_ck"
DEMO_SC = f"{RUTA_VOL}/_demo_schema"
dbutils.fs.mkdirs(DEMO_IN)


def lote(nombre, minuto, n=5, extra=None, retraso_min=0):
    """Escribe un fichero JSON con n lecturas del minuto 10:mm. extra añade un campo nuevo, retraso_min las fecha antes."""
    filas = []
    for i in range(n):
        ts = dt.datetime(2026, 11, 2, 10, minuto - retraso_min, i, tzinfo=dt.timezone.utc)
        r = {"turbine_id": f"WTG-000{i + 1}", "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "signal": "temp_gearbox_c", "value": 60.0 + i}
        if extra:
            r.update(extra)
        filas.append(json.dumps(r))
    with open(f"{DEMO_IN}/{nombre}.json", "w") as f:
        f.write("\n".join(filas))
    print("escrito", nombre)


def filas_bronze():
    return spark.table(t("demo_stream_bronze")).count() if spark.catalog.tableExists(t("demo_stream_bronze")) else 0


def bronze_stream():
    antes = filas_bronze()
    s = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", DEMO_SC).option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaHints", "ts TIMESTAMP, value DOUBLE").load(DEMO_IN))
    q = (s.writeStream.option("checkpointLocation", f"{DEMO_CK}/bronze").option("mergeSchema", "true")
         .trigger(availableNow=True).toTable(t("demo_stream_bronze")))
    q.awaitTermination()
    print(f"bronze: {filas_bronze() - antes} filas nuevas")
    return q


def gold_stream():
    # Se lanza dos veces seguidas. Con availableNow, una ventana que el watermark acaba de cerrar
    # puede no escribirse hasta la siguiente ejecución. La segunda, sin datos nuevos, solo emite esas ventanas.
    for _ in range(2):
        w = (spark.readStream.table(t("demo_stream_bronze")).withWatermark("ts", "2 minutes")
             .groupBy(F.window("ts", "1 minute"), "turbine_id").agg(F.max("value").alias("temp_max"), F.count("*").alias("n"))
             .select(F.col("window.start").alias("minuto"), "turbine_id", "temp_max", "n"))
        (w.writeStream.outputMode("append").option("checkpointLocation", f"{DEMO_CK}/gold")
          .trigger(availableNow=True).toTable(t("demo_stream_gold"))).awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint: la memoria del stream

# COMMAND ----------

lote("lote_01", 1)
lote("lote_02", 2)
q = bronze_stream()
print("filas en la tabla:", spark.table(t("demo_stream_bronze")).count())   # 10

# COMMAND ----------

lote("lote_03", 3)
q = bronze_stream()
print("filas en la tabla:", spark.table(t("demo_stream_bronze")).count())   # 15, no 25: el checkpoint sabe qué ficheros ya leyó
display(dbutils.fs.ls(f"{DEMO_CK}/bronze"))                                  # offsets, commits, sources

# COMMAND ----------

# Vigilar un stream: el último informe de progreso (filas de entrada, duración, offsets).
q.lastProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermarks: cuánto esperar a los datos tardíos

# COMMAND ----------

lote("lote_04", 4)
bronze_stream()
gold_stream()
display(spark.sql("SELECT minuto, sum(n) AS lecturas FROM demo_stream_gold GROUP BY ALL ORDER BY 1"))   # solo las ventanas ya cerradas

# COMMAND ----------

lote("lote_05", 9)                     # el tiempo avanza: el watermark pasa a las 10:07
lote("lote_tarde", 9, retraso_min=8)   # lecturas de las 10:01 que llegan ahora, por debajo del watermark
bronze_stream()
gold_stream()
display(spark.sql("SELECT minuto, sum(n) AS lecturas FROM demo_stream_gold GROUP BY ALL ORDER BY 1"))
en_bronze = spark.sql("SELECT count(*) FROM demo_stream_bronze WHERE ts >= '2026-11-02 10:01:00' AND ts < '2026-11-02 10:02:00'").first()[0]
print(f"Lecturas del minuto 10:01 en Bronze: {en_bronze}. En Gold siguen siendo 5: las tardías se han descartado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader: ingesta incremental de ficheros

# COMMAND ----------

lote("lote_06", 10, extra={"firmware_build": "2026.11-r7"})
try:
    bronze_stream()
except Exception as e:
    print("El stream se ha parado por la columna nueva. Es lo esperado: se relanza en la celda siguiente.")

# COMMAND ----------

bronze_stream()                                       # al relanzarlo, Auto Loader ya conoce la columna nueva
spark.table(t("demo_stream_bronze")).printSchema()   # aparece firmware_build

# COMMAND ----------

# MAGIC %md
# MAGIC ## foreachBatch: lógica batch dentro de un stream

# COMMAND ----------

from delta.tables import DeltaTable

spark.sql("CREATE OR REPLACE TABLE demo_ultima_lectura (turbine_id STRING, ts TIMESTAMP, value DOUBLE)")


DESTINO = t("demo_ultima_lectura")


def upsert(lote_df, batch_id):
    # En serverless la función corre en el servidor: se usa la sesión del propio lote, no la del notebook
    ult = lote_df.groupBy("turbine_id").agg(F.max("ts").alias("ts"), F.max_by("value", "ts").alias("value"))
    (DeltaTable.forName(lote_df.sparkSession, DESTINO).alias("t")
       .merge(ult.alias("s"), "t.turbine_id = s.turbine_id")
       .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


(spark.readStream.table(t("demo_stream_bronze")).select("turbine_id", "ts", "value")
   .writeStream.foreachBatch(upsert).option("checkpointLocation", f"{DEMO_CK}/upsert")
   .trigger(availableNow=True).start().awaitTermination())
display(spark.table(t("demo_ultima_lectura")))   # una fila por turbina, con la lectura de las 10:10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations: calidad declarada

# COMMAND ----------

TABLA_PIPELINE = f"{CATALOGO}.{ESQUEMA_PIPELINE}.silver_telemetria"   # el pipeline escribe en el esquema ..._pipeline
try:
    display(spark.sql(f"""
        SELECT timestamp, details:flow_progress.data_quality.expectations
        FROM event_log(TABLE({TABLA_PIPELINE}))
        WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 20"""))
except Exception as e:
    print(f"{TABLA_PIPELINE} no es una tabla de un pipeline o el pipeline no se ha ejecutado:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros y paso de valores entre tareas

# COMMAND ----------

dbutils.widgets.text("fecha_proceso", "2026-11-02")
print("parámetro:", dbutils.widgets.get("fecha_proceso"))
dbutils.jobs.taskValues.set(key="filas", value=123)
print("task value (fuera de un job usa debugValue):",
      dbutils.jobs.taskValues.get(taskKey="otra", key="filas", default=0, debugValue=-1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuando algo falla: repair run

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT job_id, run_id, result_state, period_start_time, period_end_time
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= current_date() - 7
# MAGIC ORDER BY period_start_time DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza
