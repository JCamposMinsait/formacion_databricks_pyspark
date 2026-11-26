# Databricks notebook source
# MAGIC %md
# MAGIC # Día 4: Streaming, pipelines y Lakeflow Jobs
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="4"

# COMMAND ----------

import json, datetime as dt

DEMO_IN = f"{RUTA_VOL}/demo_stream_in"    # donde llegan los ficheros
DEMO_CK = f"{RUTA_VOL}/_demo_ck"          # checkpoints de los streams
DEMO_SC = f"{RUTA_VOL}/_demo_schema"      # donde Auto Loader guarda el esquema que va viendo
dbutils.fs.mkdirs(DEMO_IN)


def lote(nombre, minuto, n=5, extra=None, retraso_min=0):
    """Escribe en DEMO_IN un fichero JSON con n lecturas del minuto 10:mm.
    extra cambia o añade campos. retraso_min fecha las lecturas unos minutos antes (datos que llegan tarde)."""
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoint: la memoria del stream
# MAGIC Un stream con `trigger(availableNow=True)` procesa lo que haya nuevo y se para, así que se puede programar como un batch. El checkpoint es la carpeta donde apunta qué ha procesado: al relanzarlo, sigue donde lo dejó. Un checkpoint por stream, y no se borra.

# COMMAND ----------

lote("lote_01", 1)
lote("lote_02", 2)
display(dbutils.fs.ls(DEMO_IN))

# COMMAND ----------

# Escribe una función cargar_bronze() que lea DEMO_IN con Auto Loader y lo escriba en demo_stream_bronze:
# - cloudFiles con formato json, schemaLocation DEMO_SC, inferColumnTypes y schemaHints "ts TIMESTAMP, value DOUBLE".
# - Escritura con checkpoint en DEMO_CK/bronze, mergeSchema y trigger availableNow.
# - Que espere a que acabe y devuelva la consulta.
# Llámala, guarda la consulta en q y cuenta las filas de demo_stream_bronze.

# COMMAND ----------

lote("lote_03", 3)
q = cargar_bronze()
print("filas en la tabla:", spark.table(t("demo_stream_bronze")).count())
display(dbutils.fs.ls(f"{DEMO_CK}/bronze"))

# COMMAND ----------

# Vigilar un stream: el último informe de progreso
q.lastProgress

# COMMAND ----------

# MAGIC %md
# MAGIC ## Watermarks: cuánto esperar a los datos tardíos
# MAGIC Una agregación por ventanas de tiempo en streaming no sabe cuándo una ventana está completa: siempre puede llegar una lectura atrasada. El watermark le dice cuánto esperar: "lo que llegue con más de 2 minutos de retraso respecto a lo último que he visto, se descarta". Con eso puede cerrar ventanas, escribirlas y olvidarse de ellas.
# MAGIC
# MAGIC La Gold de abajo calcula la temperatura máxima por turbina y minuto, con un watermark de 2 minutos.

# COMMAND ----------

def gold_stream():
    # Se lanza dos veces seguidas. Con availableNow, una ventana que el watermark acaba de cerrar
    # puede no escribirse hasta la siguiente ejecución. La segunda, sin datos nuevos, solo emite esas ventanas.
    for _ in range(2):
        w = (spark.readStream.table(t("demo_stream_bronze")).withWatermark("ts", "2 minutes")
             .groupBy(F.window("ts", "1 minute"), "turbine_id").agg(F.max("value").alias("temp_max"), F.count("*").alias("n"))
             .select(F.col("window.start").alias("minuto"), "turbine_id", "temp_max", "n"))
        (w.writeStream.outputMode("append").option("checkpointLocation", f"{DEMO_CK}/gold")
          .trigger(availableNow=True).toTable(t("demo_stream_gold"))).awaitTermination()


lote("lote_04", 4)
cargar_bronze()
gold_stream()
display(spark.sql("SELECT minuto, sum(n) AS lecturas FROM demo_stream_gold GROUP BY ALL ORDER BY 1"))   # solo las ventanas ya cerradas

# COMMAND ----------

lote("lote_05", 9)                     # el tiempo avanza: el watermark pasa a las 10:07
lote("lote_tarde", 9, retraso_min=8)   # lecturas de las 10:01 que llegan ahora, por debajo del watermark
cargar_bronze()
gold_stream()
display(spark.sql("SELECT minuto, sum(n) AS lecturas FROM demo_stream_gold GROUP BY ALL ORDER BY 1"))
en_bronze = spark.sql("SELECT count(*) FROM demo_stream_bronze WHERE ts >= '2026-11-02 10:01:00' AND ts < '2026-11-02 10:02:00'").first()[0]
print(f"Lecturas del minuto 10:01 en Bronze: {en_bronze}. En Gold siguen siendo 5.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader: ingesta incremental de ficheros
# MAGIC Si llega un campo nuevo, Auto Loader para el stream y apunta la columna en su esquema. Al relanzarlo sigue con ella. Para a propósito, para que el cambio no pase sin que nadie se entere.

# COMMAND ----------

lote("lote_06", 10, extra={"firmware_build": "2026.11-r7"})
try:
    cargar_bronze()
except Exception as e:
    print("El stream se ha parado por la columna nueva. Es lo esperado: se relanza en la celda siguiente.")

# COMMAND ----------

cargar_bronze()                                       # al relanzarlo, Auto Loader ya conoce la columna nueva
spark.table(t("demo_stream_bronze")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## foreachBatch: lógica batch dentro de un stream
# MAGIC Algunas cosas no se pueden escribir como un stream, por ejemplo un `MERGE`. Con `foreachBatch`, Spark le pasa a una función cada lote de filas nuevas como un DataFrame normal, y dentro se puede hacer cualquier cosa. Aquí, un `MERGE` que deja en una tabla la última lectura de cada turbina.

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
display(spark.table(t("demo_ultima_lectura")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectations: calidad declarada
# MAGIC En un pipeline declarativo, las reglas de calidad se escriben como decoradores sobre la tabla. `expect` solo cuenta las filas que no cumplen, `expect_or_drop` las descarta y `expect_or_fail` para el pipeline. El código está en `pipeline_demo.py`, en esta carpeta.

# COMMAND ----------

# Unas cuantas lecturas malas para que las reglas del pipeline tengan algo que hacer
lote("lote_07", 11)
lote("lote_nulos", 12, n=2, extra={"value": None})         # sin valor
lote("lote_centinela", 13, n=3, extra={"value": -9999})    # el -9999 del sensor
lote("lote_caliente", 14, n=1, extra={"value": 130.0})     # temperatura rara

# COMMAND ----------

# En el lab 4c se crea desde Jobs & Pipelines. Aquí se crea con el SDK, que es lo mismo que haría un script de despliegue.
import os, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

w = WorkspaceClient()
NOMBRE_PIPELINE = f"demo-pipeline-{ALUMNO}"
spec = dict(
    name=NOMBRE_PIPELINE, catalog=CATALOGO, schema=ESQUEMA_PIPELINE,   # el esquema ..._pipeline
    serverless=True, continuous=False, development=True,
    libraries=[PipelineLibrary(file=FileLibrary(path=os.path.join(os.getcwd(), "pipeline_demo.py")))],
    configuration={"ruta_demo": DEMO_IN},
)
ya_existe = list(w.pipelines.list_pipelines(filter=f"name LIKE '{NOMBRE_PIPELINE}'"))
if ya_existe:
    pipeline_id = ya_existe[0].pipeline_id
    w.pipelines.update(pipeline_id=pipeline_id, **spec)
else:
    pipeline_id = w.pipelines.create(**spec).pipeline_id
print("Pipeline:", f"{w.config.host}/pipelines/{pipeline_id}")

# COMMAND ----------

# Lanza el pipeline con full refresh y espera a que acabe. Mientras corre, abrid el enlace de arriba.
update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True).update_id
while (estado := w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value) not in ("COMPLETED", "FAILED", "CANCELED"):
    time.sleep(10)
print("Ejecución:", estado)
if estado != "COMPLETED":
    raise Exception("El pipeline no ha terminado bien. Mira el error en el enlace de arriba. "
                    "En la cuenta gratuita solo puede haber un pipeline activo: para el del lab 4c si está en marcha.")

# COMMAND ----------

TABLA_PIPELINE = f"{CATALOGO}.{ESQUEMA_PIPELINE}.demo_pipeline_silver"
print("bronze:", spark.table(f"{CATALOGO}.{ESQUEMA_PIPELINE}.demo_pipeline_bronze").count())
print("silver:", spark.table(TABLA_PIPELINE).count())
display(spark.sql(f"""
    SELECT timestamp, details:flow_progress.data_quality.expectations
    FROM event_log(TABLE({TABLA_PIPELINE}))
    WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
    ORDER BY timestamp DESC
    LIMIT 20"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parámetros y paso de valores entre tareas
# MAGIC Un job pasa parámetros a sus notebooks como widgets. Entre tareas, los valores calculados viajan con `taskValues`: una tarea los publica y las siguientes los leen. Fuera de un job, `get` devuelve el `debugValue`, para poder probar el notebook suelto.

# COMMAND ----------

dbutils.widgets.text("fecha_proceso", "2026-11-02")
print("parámetro:", dbutils.widgets.get("fecha_proceso"))
dbutils.jobs.taskValues.set(key="filas", value=123)
print("task value (fuera de un job usa debugValue):",
      dbutils.jobs.taskValues.get(taskKey="otra", key="filas", default=0, debugValue=-1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuando algo falla: repair run
# MAGIC Si falla una tarea de un job, **Repair run** relanza solo esa y las que dependen de ella, sin repetir las que ya fueron bien. El historial de ejecuciones de todos los jobs está en las tablas de sistema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT job_id, run_id, result_state, period_start_time, period_end_time
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE period_start_time >= current_date() - 7
# MAGIC ORDER BY period_start_time DESC
# MAGIC LIMIT 50
