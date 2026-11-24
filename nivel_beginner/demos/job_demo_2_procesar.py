# Databricks notebook source
# MAGIC %md
# MAGIC # Job de la demo: tarea 2, procesar

# COMMAND ----------

dbutils.widgets.text("fallar", "no")
lecturas = dbutils.jobs.taskValues.get(taskKey="preparar", key="lecturas", debugValue=0)
if dbutils.widgets.get("fallar") == "si":
    raise Exception("Fallo provocado para enseñar el repair run")
print(f"Procesadas {lecturas} lecturas")
