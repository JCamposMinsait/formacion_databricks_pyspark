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

TABLA_PIPELINE = f"{CATALOGO}.{ESQUEMA_PIPELINE}.silver_telemetria"
try:
    display(spark.sql(f"""
        SELECT timestamp, details:flow_progress.data_quality.expectations AS expectations
        FROM event_log(TABLE({TABLA_PIPELINE}))
        WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
        ORDER BY timestamp DESC LIMIT 20"""))
except Exception as e:
    print("Todavía no hay un pipeline con esa tabla:", str(e).splitlines()[0][:150])
