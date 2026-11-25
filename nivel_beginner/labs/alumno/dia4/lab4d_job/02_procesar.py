# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4d · Tarea 2 · Procesar
# MAGIC Lee de la tarea `configurar` qué parques revisar y calcula la temperatura máxima de multiplicadora
# MAGIC por turbina en el último día con datos. Las que superan el umbral van a una tabla de alertas.
# MAGIC
# MAGIC `debugValue` es lo que devuelve `taskValues.get` cuando ejecutas el notebook a mano, fuera de un job.

# COMMAND ----------

# MAGIC %run ../../../00_setup/00_config

# COMMAND ----------

parques = dbutils.jobs.taskValues.get(taskKey="configurar", key="parques", default=["P01"], debugValue=["P01", "P02", "P03"])
umbral  = dbutils.jobs.taskValues.get(taskKey="configurar", key="umbral", default=78.0, debugValue=78.0)

temps = (spark.table(t("silver_telemetria"))
         .filter((F.col("signal") == "temp_gearbox_c") & F.col("park_id").isin(parques)))

ultimo_dia = temps.agg(F.max(F.to_date("ts"))).first()[0]

alertas = (temps.filter(F.to_date("ts") == ultimo_dia)
           .groupBy("turbine_id", "park_id")
           .agg(F.round(F.max("value"), 1).alias("temp_max"))
           .filter(F.col("temp_max") > umbral)
           .withColumn("fecha", F.lit(ultimo_dia))
           .withColumn("umbral", F.lit(umbral)))

alertas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("job_alertas_temperatura"))
n = spark.table(t("job_alertas_temperatura")).count()
print(f"Día {ultimo_dia}: {n} turbinas por encima de {umbral} ºC")

dbutils.jobs.taskValues.set(key="n_alertas", value=n)
dbutils.jobs.taskValues.set(key="tabla_alertas", value=t("job_alertas_temperatura"))
