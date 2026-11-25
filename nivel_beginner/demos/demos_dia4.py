# Databricks notebook source
# MAGIC %md
# MAGIC # Día 4: Automatizar ingesta, jobs y pipelines

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader y checkpoint

# COMMAND ----------

lote("lote_01", 1)
lote("lote_02", 2)
bronze_stream()
print("filas en la tabla:", spark.table(t("demo_stream_bronze")).count())   # 10

# COMMAND ----------

lote("lote_03", 3)
bronze_stream()
print("filas en la tabla:", spark.table(t("demo_stream_bronze")).count())   # 15, no 25: el checkpoint sabe qué ficheros ya leyó

# COMMAND ----------

display(dbutils.fs.ls(f"{DEMO_CK}/bronze"))                                  # offsets, commits y sources

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Un campo nuevo

# COMMAND ----------

lote("lote_04", 4, extra={"firmware_build": "2026.11-r7"})
try:
    bronze_stream()
except Exception as e:
    print("El stream se ha parado por la columna nueva. Es lo esperado: se relanza en la celda siguiente.")

# COMMAND ----------

bronze_stream()                                       # al relanzarlo, Auto Loader ya conoce la columna nueva
spark.table(t("demo_stream_bronze")).printSchema()   # aparece firmware_build

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Un job

# COMMAND ----------

# Las dos tareas del job son los notebooks job_demo_1_preparar y job_demo_2_procesar, en esta misma carpeta.
# Así se pasan un valor: la primera lo publica y la segunda lo lee. Fuera de un job, se usa el debugValue.
dbutils.jobs.taskValues.set(key="lecturas", value=15)
print(dbutils.jobs.taskValues.get(taskKey="preparar", key="lecturas", debugValue=-1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Un pipeline

# COMMAND ----------

# Unas cuantas lecturas malas para que las reglas del pipeline tengan algo que hacer
lote("lote_05", 5)
lote("lote_nulos", 6, n=2, extra={"value": None})         # sin valor: se descartan
lote("lote_centinela", 7, n=3, extra={"value": -9999})    # el -9999 del sensor: se descartan
lote("lote_caliente", 8, n=1, extra={"value": 130.0})     # pasa, pero cuenta como aviso

# COMMAND ----------

# El código del pipeline está en pipeline_demo.py, en esta carpeta. Lo normal es crearlo desde
# Jobs & Pipelines, como en el lab 4c. Aquí se crea con el SDK para no perder tiempo en la demo.
import os, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

w = WorkspaceClient()
NOMBRE_PIPELINE = f"demo-pipeline-{ALUMNO}"
spec = dict(
    name=NOMBRE_PIPELINE, catalog=CATALOGO, schema=ESQUEMA_PIPELINE,
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

# Full refresh porque la limpieza del principio vuelve a escribir los mismos ficheros.
# Mientras corre, abre el enlace de arriba: se ve el grafo y, al acabar, la pestaña de calidad de demo_pipeline_silver.
update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True).update_id
while (estado := w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state.value) not in ("COMPLETED", "FAILED", "CANCELED"):
    time.sleep(10)
print("Ejecución:", estado)
if estado != "COMPLETED":
    raise Exception("El pipeline no ha terminado bien. Mira el error en el enlace de arriba. "
                    "En la cuenta gratuita solo puede haber un pipeline activo: para el del lab 4c si está en marcha.")

# COMMAND ----------

TABLA_PIPELINE = f"{CATALOGO}.{ESQUEMA_PIPELINE}.demo_pipeline_silver"
print("bronze:", spark.table(f"{CATALOGO}.{ESQUEMA_PIPELINE}.demo_pipeline_bronze").count())   # 31
print("silver:", spark.table(TABLA_PIPELINE).count())                                          # 26: faltan los nulos y los -9999

# Lo mismo que enseña la pestaña de calidad, sacado del registro de eventos del pipeline
display(spark.sql(f"""
    SELECT timestamp, details:flow_progress.data_quality.expectations AS expectations
    FROM event_log(TABLE({TABLA_PIPELINE}))
    WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
    ORDER BY timestamp DESC LIMIT 20"""))
