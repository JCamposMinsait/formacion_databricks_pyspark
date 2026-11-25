# Databricks notebook source
# MAGIC %md
# MAGIC ### Tarea 1 · Foto del maestro de turbinas
# MAGIC En un caso real se leería del ERP por JDBC o con Lakeflow Connect. Aquí lo generamos para que el bundle no dependa de nada externo.

# COMMAND ----------

dbutils.widgets.text("catalogo", "formacion")
dbutils.widgets.text("esquema", "eolica")
cat, esq = dbutils.widgets.get("catalogo"), dbutils.widgets.get("esquema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cat}.{esq}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {cat}.{esq}.landing")

import random
rng = random.Random(42)    # la misma secuencia que df_turbinas() de 00_config, para que cada turbina tenga su parque y su modelo de siempre
azar = random.Random()     # sin semilla: cada ejecución pone en mantenimiento turbinas distintas y la SCD2 tiene cambios que guardar
modelos = [("AE-4200", 4200), ("BX-4500", 4500), ("CW-5000", 5000)]
filas = []
for i in range(1, 241):
    park = "P01" if i <= 144 else rng.choice(["P02", "P03", "P04", "P05", "P06"])
    m, kw = rng.choice(modelos)
    rng.randint(2012, 2025), rng.randint(1, 12)    # año y mes de puesta en marcha en df_turbinas(). Aquí no se usan, pero hay que consumirlos
    filas.append((f"WTG-{i:04d}", park, m, kw, "MANTENIMIENTO" if azar.random() < 0.05 else "OPERATIVA"))

(spark.createDataFrame(filas, "turbine_id string, park_id string, modelo string, potencia_nominal_kw int, estado string")
     .write.mode("overwrite").saveAsTable(f"{cat}.{esq}.maestro_turbinas_snapshot"))
