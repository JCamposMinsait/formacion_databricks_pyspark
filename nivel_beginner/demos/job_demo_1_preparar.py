# Databricks notebook source
# MAGIC %md
# MAGIC # Job de la demo: tarea 1, preparar

# COMMAND ----------

dbutils.widgets.text("fecha_proceso", "2026-11-02")
fecha = dbutils.widgets.get("fecha_proceso")
lecturas = 1440 * 60                                        # lo que haya calculado esta tarea
dbutils.jobs.taskValues.set(key="lecturas", value=lecturas)
print(f"Preparado el día {fecha}: {lecturas} lecturas")
