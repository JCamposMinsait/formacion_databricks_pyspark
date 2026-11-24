# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4d · Tarea 1 · Configurar
# MAGIC Recibe los parámetros del job (widgets) y decide qué parques se revisan.
# MAGIC Pasa el resultado a las siguientes tareas con `dbutils.jobs.taskValues`.

# COMMAND ----------

dbutils.widgets.dropdown("zona", "SUR", ["SUR", "NORTE", "TODAS"])
dbutils.widgets.text("umbral_temp", "78")

zona   = dbutils.widgets.get("zona")
umbral = float(dbutils.widgets.get("umbral_temp"))

PARQUES_POR_ZONA = {"SUR": ["P01", "P02", "P03"], "NORTE": ["P04", "P05", "P06"]}
parques = PARQUES_POR_ZONA["SUR"] + PARQUES_POR_ZONA["NORTE"] if zona == "TODAS" else PARQUES_POR_ZONA[zona]

print(f"Zona: {zona} · parques: {parques} · umbral: {umbral} ºC")

# Valores para las siguientes tareas (tienen que ser serializables a JSON)
dbutils.jobs.taskValues.set(key="parques", value=parques)
dbutils.jobs.taskValues.set(key="umbral", value=umbral)
