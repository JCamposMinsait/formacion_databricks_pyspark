# Databricks notebook source
# MAGIC %md
# MAGIC ### Tarea 5 · Notificar a mantenimiento (solo se ejecuta si hay alertas)
# MAGIC Aquí iría la llamada al GMAO / Teams / ServiceNow. Para el curso, pintamos la lista.

# COMMAND ----------

dbutils.widgets.text("catalogo", "formacion")
dbutils.widgets.text("esquema", "eolica")
cat, esq = dbutils.widgets.get("catalogo"), dbutils.widgets.get("esquema")
display(spark.table(f"{cat}.{esq}.alertas_mantenimiento"))
