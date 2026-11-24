# Databricks notebook source
# MAGIC %md
# MAGIC ### Tarea 3 · Control de calidad + alertas
# MAGIC Publica un *task value* (`alertas`) que usa la `condition_task` siguiente para decidir si notifica.
# MAGIC Mira la última hora **con datos**, no la hora actual: así funciona igual con el simulador en marcha que con ficheros de otro día.

# COMMAND ----------

dbutils.widgets.text("catalogo", "formacion")
dbutils.widgets.text("esquema", "eolica")
cat, esq = dbutils.widgets.get("catalogo"), dbutils.widgets.get("esquema")

# Control duro: si el pipeline no ha generado nada, fallamos el job (y salta la notificación)
ultimo = spark.sql(f"SELECT max(minuto) AS m FROM {cat}.{esq}.gold_turbina_minuto").first().m
if ultimo is None:
    raise Exception("El pipeline no ha generado datos. Revisad el landing y el pipeline.")

alertas = spark.sql(f"""
  SELECT turbine_id, max(temp_gearbox_c) AS temp_max
  FROM {cat}.{esq}.gold_turbina_minuto
  WHERE minuto >= (SELECT max(minuto) FROM {cat}.{esq}.gold_turbina_minuto) - INTERVAL 1 HOUR
  GROUP BY turbine_id HAVING max(temp_gearbox_c) > 75
""")
n = alertas.count()
alertas.write.mode("overwrite").saveAsTable(f"{cat}.{esq}.alertas_mantenimiento")
print(f"Último minuto con datos: {ultimo}. Turbinas en alerta en la hora anterior: {n}")
dbutils.jobs.taskValues.set(key="alertas", value=n)
