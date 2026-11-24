# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4d · Tarea 4 · Alertar
# MAGIC Solo se ejecuta si la condición del job (`n_alertas > 0`) es verdadera.
# MAGIC Con el parámetro `modo_estricto = true` el notebook falla a propósito para practicar notificaciones y **Repair run**.

# COMMAND ----------

dbutils.widgets.dropdown("modo_estricto", "false", ["true", "false"])

tabla = dbutils.jobs.taskValues.get(taskKey="procesar", key="tabla_alertas", default="", debugValue="")
n     = dbutils.jobs.taskValues.get(taskKey="procesar", key="n_alertas", default=0, debugValue=0)

print(f"Turbinas en alerta: {n}")
if tabla:
    display(spark.table(tabla).orderBy("temp_max", ascending=False))

if dbutils.widgets.get("modo_estricto") == "true" and n > 0:
    raise Exception(f"{n} turbinas superan el umbral de temperatura. Revisad la tabla {tabla}.")
