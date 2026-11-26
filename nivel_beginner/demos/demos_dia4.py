# Databricks notebook source
# MAGIC %md
# MAGIC # Día 4: Automatizar ingesta, jobs y pipelines
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="4"

# COMMAND ----------

import json, datetime as dt

DEMO_IN = f"{RUTA_VOL}/demo_stream_in"    # donde llegan los ficheros
DEMO_CK = f"{RUTA_VOL}/_demo_ck"          # checkpoint del stream
DEMO_SC = f"{RUTA_VOL}/_demo_schema"      # donde Auto Loader guarda el esquema que va viendo
dbutils.fs.mkdirs(DEMO_IN)


def lote(nombre, minuto, n=5, extra=None):
    """Escribe en DEMO_IN un fichero JSON con n lecturas del minuto 10:mm. extra cambia o añade campos."""
    filas = []
    for i in range(n):
        ts = dt.datetime(2026, 11, 2, 10, minuto, i, tzinfo=dt.timezone.utc)
        r = {"turbine_id": f"WTG-000{i + 1}", "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "signal": "temp_gearbox_c", "value": 60.0 + i}
        if extra:
            r.update(extra)
        filas.append(json.dumps(r))
    with open(f"{DEMO_IN}/{nombre}.json", "w") as f:
        f.write("\n".join(filas))
    print("escrito", nombre)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Auto Loader y checkpoint
# MAGIC Auto Loader lee los ficheros que van llegando a una carpeta. Es un stream (`readStream` con formato `cloudFiles`), pero se puede lanzar como un proceso por lotes: con `trigger(availableNow=True)` lee lo que haya nuevo y se para.
# MAGIC
# MAGIC El **checkpoint** es una carpeta donde el stream apunta qué ficheros ya ha procesado. Si se relanza, solo lee los nuevos. Sin checkpoint, cada ejecución volvería a cargar todo.

# COMMAND ----------

# Llegan dos ficheros con 5 lecturas cada uno
lote("lote_01", 1)
lote("lote_02", 2)
display(dbutils.fs.ls(DEMO_IN))

# COMMAND ----------

# Escribe una función cargar_bronze() que:
# 1. Lea DEMO_IN con Auto Loader: readStream, formato cloudFiles, cloudFiles.format json,
#    cloudFiles.schemaLocation DEMO_SC y cloudFiles.schemaHints "ts TIMESTAMP, value DOUBLE".
# 2. Lo escriba en la tabla demo_stream_bronze con checkpoint en DEMO_CK/bronze, mergeSchema y trigger availableNow.
# 3. Espere a que acabe (awaitTermination).
# Llámala y cuenta las filas de demo_stream_bronze.

# COMMAND ----------

# Llega un tercer fichero y se relanza la carga
lote("lote_03", 3)
cargar_bronze()
print("filas en la tabla:", spark.table(t("demo_stream_bronze")).count())

# COMMAND ----------

display(dbutils.fs.ls(f"{DEMO_CK}/bronze"))   # lo que guarda el checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Un campo nuevo
# MAGIC Si un día los ficheros traen un campo que no existía, Auto Loader para el stream y apunta la columna nueva en su esquema. Al relanzarlo, sigue con la columna ya incluida. Para un momento a propósito: así el cambio no pasa sin que nadie se entere.

# COMMAND ----------

lote("lote_04", 4, extra={"firmware_build": "2026.11-r7"})
try:
    cargar_bronze()
except Exception as e:
    print("El stream se ha parado por la columna nueva. Es lo esperado: se relanza en la celda siguiente.")

# COMMAND ----------

cargar_bronze()                                       # al relanzarlo, Auto Loader ya conoce la columna nueva
spark.table(t("demo_stream_bronze")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Un job
# MAGIC Un job ejecuta una o varias tareas (notebooks, pipelines, scripts...) en orden, con horario, reintentos y avisos. El job de la demo se crea a mano en **Jobs & Pipelines** con dos tareas: `job_demo_1_preparar` y `job_demo_2_procesar`, de esta carpeta.
# MAGIC
# MAGIC Las tareas se pasan valores con `taskValues`: una los publica y la siguiente los lee. Si una tarea falla, **Repair run** relanza solo la que ha fallado y las que dependen de ella.

# COMMAND ----------

# Así se pasan un valor: la primera tarea lo publica y la segunda lo lee.
# Fuera de un job no hay tarea anterior, así que get devuelve el debugValue.
dbutils.jobs.taskValues.set(key="lecturas", value=15)
print(dbutils.jobs.taskValues.get(taskKey="preparar", key="lecturas", debugValue=-1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Un pipeline
# MAGIC En un pipeline declarativo se escribe qué tablas hay y de dónde sale cada una. Databricks deduce el orden, crea las tablas, gestiona los checkpoints y lleva la cuenta de la calidad. Las **expectations** son reglas sobre los datos: unas solo avisan y otras descartan las filas que no las cumplen.
# MAGIC
# MAGIC El código está en `pipeline_demo.py`, en esta carpeta: una Bronze con Auto Loader y una Silver con tres reglas.

# COMMAND ----------

# Unas cuantas lecturas malas para que las reglas del pipeline tengan algo que hacer
lote("lote_05", 5)
lote("lote_nulos", 6, n=2, extra={"value": None})         # sin valor
lote("lote_centinela", 7, n=3, extra={"value": -9999})    # el -9999 del sensor
lote("lote_caliente", 8, n=1, extra={"value": 130.0})     # temperatura rara

# COMMAND ----------

# Lo normal es crear el pipeline desde Jobs & Pipelines, como en el lab 4c. Aquí se crea con el SDK
# para no perder tiempo en la demo. Si ya existe de otra pasada, se actualiza.
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

# Lanza el pipeline y espera a que acabe. Mientras corre, abrid el enlace de arriba para ver el grafo.
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

# COMMAND ----------

# MAGIC %md
# MAGIC Lo mismo que enseña la pestaña de calidad del pipeline, sacado de su registro de eventos.

# COMMAND ----------

display(spark.sql(f"""
    SELECT timestamp, details:flow_progress.data_quality.expectations AS expectations
    FROM event_log(TABLE({TABLA_PIPELINE}))
    WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
    ORDER BY timestamp DESC LIMIT 20"""))
