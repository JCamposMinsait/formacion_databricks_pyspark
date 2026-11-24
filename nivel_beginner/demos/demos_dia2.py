# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2: PySpark y rendimiento básico

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="2"

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

# Copia en JSON para la demo de lectura
RUTA_JSON = f"{RUTA_VOL}/demo_dia2/json"
spark.table(t("demo_telemetria")).write.mode("overwrite").json(RUTA_JSON)
lect = spark.table(t("demo_telemetria"))
maestro = df_turbinas().join(df_parques().select("park_id", F.col("nombre").alias("parque")), "park_id")


def filas_por_particion(df):
    return df.groupBy(F.spark_partition_id().alias("particion")).count().orderBy("particion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer

# COMMAND ----------

t0 = time.time()
inferido = spark.read.json(RUTA_JSON)
inferido.printSchema()
print(f"esquema inferido: {time.time() - t0:.1f} s")

t0 = time.time()
explicito = spark.read.schema("turbine_id STRING, park_id STRING, ts TIMESTAMP, signal STRING, value DOUBLE").json(RUTA_JSON)
explicito.printSchema()
print(f"esquema escrito: {time.time() - t0:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformar

# COMMAND ----------

display(lect
        .filter(F.col("signal") == "power_kw")
        .select("turbine_id", "ts", "value")
        .withColumn("potencia_mw", F.round(F.col("value") / 1000, 3))
        .withColumn("estado", F.when(F.col("value") > 3000, "ALTA").when(F.col("value") > 0, "BAJA").otherwise("PARADA"))
        .limit(10))

# COMMAND ----------

sucio = spark.createDataFrame([("WTG-0001", "12.5"), ("WTG-0002", "N/A"), ("WTG-0003", None), ("WTG-0004", "-9999")],
                              "turbine_id string, valor string")
display(sucio
        .withColumn("como_numero", F.expr("try_cast(valor AS double)"))              # N/A pasa a nulo en vez de fallar
        .withColumn("sin_centinela", F.when(F.col("como_numero") != -9999, F.col("como_numero")))
        .withColumn("con_cero", F.coalesce("sin_centinela", F.lit(0.0))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agrupar y unir

# COMMAND ----------

potencia = lect.filter("signal = 'power_kw'")
display(potencia.groupBy("park_id")
        .agg(F.round(F.avg("value"), 1).alias("kw_medio"), F.count("*").alias("lecturas"))
        .orderBy("park_id"))

# COMMAND ----------

display(potencia.join(maestro.select("turbine_id", "parque", "modelo"), "turbine_id")
        .groupBy("parque", "modelo").agg(F.round(F.avg("value"), 1).alias("kw_medio"))
        .orderBy("parque", "modelo"))

# COMMAND ----------

# Una lectura de una turbina que no está en el maestro
extra = spark.createDataFrame([("WTG-0999", "power_kw", 1500.0)], "turbine_id string, signal string, value double")
display(extra.join(maestro, "turbine_id", "left_anti"))

# COMMAND ----------

# Lo mismo en SQL, con vistas temporales
potencia.createOrReplaceTempView("potencia_v")
maestro.createOrReplaceTempView("maestro_v")
display(spark.sql("""
    SELECT m.parque, round(avg(p.value), 1) AS kw_medio
    FROM potencia_v p JOIN maestro_v m USING (turbine_id)
    GROUP BY m.parque ORDER BY m.parque"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lazy y planes

# COMMAND ----------

t0 = time.time()
plan = lect.filter("signal = 'wind_speed'").withColumn("doble", F.col("value") * 2).groupBy("turbine_id").agg(F.max("doble"))
print(f"transformaciones: {time.time() - t0:.2f} s")      # no ha hecho nada todavía
t0 = time.time()
plan.count()
print(f"acción: {time.time() - t0:.2f} s")                # aquí se hace todo el trabajo

# COMMAND ----------

lect.filter("signal = 'wind_speed'").withColumn("doble", F.col("value") * 2).explain()   # narrow: sin Exchange

# COMMAND ----------

lect.groupBy("turbine_id").count().explain()                                             # wide: con Exchange (shuffle)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Particiones

# COMMAND ----------

display(filas_por_particion(lect.repartition(8)))               # 8 particiones parejas

# COMMAND ----------

display(filas_por_particion(lect.repartition(8).coalesce(2)))   # coalesce junta particiones sin repartir de nuevo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Skew y broadcast

# COMMAND ----------

grande = (spark.range(3_000_000)
          .withColumn("park_id", F.when(F.rand(1) < 0.7, "P01")
                      .otherwise(F.concat(F.lit("P0"), (F.rand(2) * 5 + 2).cast("int").cast("string")))))
display(filas_por_particion(grande.repartition(8, "park_id")))  # una partición se lleva casi todo

# COMMAND ----------

tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")
grande.join(tarifas.hint("merge"), "park_id").explain()         # sort-merge: Exchange en los dos lados

# COMMAND ----------

grande.join(F.broadcast(tarifas), "park_id").explain()           # broadcast: la tabla grande no se mueve

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. UDF o función de Spark

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import udf


def potencia_teorica(v):
    return 0.0 if v is None or v < 3 or v >= 25 else min(0.5 * 1.225 * math.pi * 75 ** 2 * v ** 3 * 0.45 / 1000, 4200.0)


con_udf = udf(potencia_teorica, "double")
nativa = (F.when((F.col("v") < 3) | (F.col("v") >= 25), 0.0)
           .otherwise(F.least(F.lit(0.5 * 1.225 * math.pi * 75 ** 2 * 0.45 / 1000) * F.pow("v", 3), F.lit(4200.0))))
datos = spark.range(0, 2_000_000).select((F.rand(1) * 20).alias("v"))
for nombre, expr in [("UDF", con_udf("v")), ("Funciones de Spark", nativa)]:
    t0 = time.time()
    datos.select(expr.alias("p")).agg(F.sum("p")).collect()
    print(f"{nombre:20s} {time.time() - t0:5.1f} s")
