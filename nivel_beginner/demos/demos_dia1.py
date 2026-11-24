# Databricks notebook source
# MAGIC %md
# MAGIC # Día 1: La plataforma y primeros pasos

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="1"

# COMMAND ----------

import math, time, datetime as dt


def generar_demo(turbinas, minutos):
    inicio = int(dt.datetime(2026, 11, 2, tzinfo=dt.timezone.utc).timestamp())
    return (turbinas.crossJoin(spark.range(0, minutos).withColumnRenamed("id", "m"))
            .withColumn("ts", F.timestamp_seconds(F.lit(inicio) + F.col("m") * 60))
            .withColumn("wind_speed", F.greatest(F.lit(0.0), F.lit(6.0) + F.lit(3.0) * F.sin(F.col("m") / 229.0) + F.randn(1) * 1.5))
            .withColumn("power_kw", F.least(F.col("potencia_nominal_kw").cast("double"),
                        F.lit(0.5 * 1.225 * math.pi * 0.40 / 1000) * F.pow(F.col("diametro_rotor_m") / 2, 2) * F.pow("wind_speed", 3)))
            .selectExpr("turbine_id", "park_id", "ts", "stack(2, 'wind_speed', wind_speed, 'power_kw', power_kw) as (signal, value)"))


# Telemetría de ejemplo: 60 turbinas, un día, dos señales (unas 173.000 filas)
(generar_demo(df_turbinas().filter("cast(substring(turbine_id, 5) as int) % 4 = 0"), 1440)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("demo_telemetria")))
print(spark.table(t("demo_telemetria")).count(), "filas en demo_telemetria")

# Un CSV pequeño en el volume, por si no se sube uno a mano en la demo
DEMO1 = f"{RUTA_VOL}/demo_dia1"
dbutils.fs.mkdirs(DEMO1)
df_parques().toPandas().to_csv(f"{DEMO1}/parques.csv", index=False)
print("CSV de ejemplo en", f"{DEMO1}/parques.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Un notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS yo, current_catalog() AS catalogo, current_schema() AS esquema

# COMMAND ----------

dbutils.widgets.text("parque", "P01")
parque = dbutils.widgets.get("parque")
print("Parque elegido:", parque)                          # cambiad el valor arriba y volved a ejecutar
print("Esta variable viene del %run de la configuración:", ESQUEMA)
display(spark.range(100))                                 # pestañas de tabla, visualización y Data Profile

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Volumes y la primera tabla

# COMMAND ----------

display(dbutils.fs.ls(DEMO1))                             # el volume se trata como una carpeta

# COMMAND ----------

parques = (spark.read
           .option("header", True)                        # la primera fila son los nombres de columna
           .option("inferSchema", True)                   # que deduzca los tipos
           .csv(f"{DEMO1}/parques.csv"))
display(parques)
parques.printSchema()

# COMMAND ----------

parques.write.mode("overwrite").saveAsTable(t("demo_parques"))   # ya es una tabla Delta en Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_parques WHERE operador = 'EnerSur'

# COMMAND ----------

# Una copia con nombre único para enseñar DROP y UNDROP
copia = f"demo_drop_{int(time.time())}"
spark.table(t("demo_parques")).write.saveAsTable(t(copia))
spark.sql(f"DROP TABLE {t(copia)}")
display(spark.sql(f"SHOW TABLES DROPPED IN {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

spark.sql(f"UNDROP TABLE {t(copia)}")                      # vuelve, porque es managed
display(spark.table(t(copia)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Formatos

# COMMAND ----------

df = spark.table(t("demo_telemetria"))
base = f"{DEMO1}/formatos"
for fmt in ["csv", "json", "parquet"]:
    escritor = df.write.mode("overwrite").format(fmt)
    if fmt == "csv":
        escritor = escritor.option("header", "true")
    escritor.save(f"{base}/{fmt}")
    total = sum(f.size for f in dbutils.fs.ls(f"{base}/{fmt}") if f.name.startswith("part-"))
    print(f"{fmt:8s} {total / 1024 / 1024:6.1f} MB")
df.write.mode("overwrite").saveAsTable(t("demo_formatos_delta"))
tam = spark.sql(f"DESCRIBE DETAIL {t('demo_formatos_delta')}").first().sizeInBytes
print(f"{'delta':8s} {tam / 1024 / 1024:6.1f} MB")

# COMMAND ----------

spark.read.option("header", "true").csv(f"{base}/csv").printSchema()   # todo string: el CSV no sabe de tipos
