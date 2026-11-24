# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2: PySpark, rendimiento y conectividad

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="2"

# COMMAND ----------

# Datos para las demos: 60 turbinas, 1 día, en EAV (unas 173.000 filas), y una copia en JSON para la demo de lectura.
import math, time, datetime as dt


def generar_demo(turbinas, minutos):
    inicio = int(dt.datetime(2026, 11, 2, tzinfo=dt.timezone.utc).timestamp())
    return (turbinas.crossJoin(spark.range(0, minutos).withColumnRenamed("id", "m"))
            .withColumn("ts", F.timestamp_seconds(F.lit(inicio) + F.col("m") * 60))
            .withColumn("wind_speed", F.greatest(F.lit(0.0), F.lit(6.0) + F.lit(3.0) * F.sin(F.col("m") / 229.0) + F.randn(1) * 1.5))
            .withColumn("power_kw", F.least(F.col("potencia_nominal_kw").cast("double"),
                        F.lit(0.5 * 1.225 * math.pi * 0.40 / 1000) * F.pow(F.col("diametro_rotor_m") / 2, 2) * F.pow("wind_speed", 3)))
            .selectExpr("turbine_id", "park_id", "ts", "stack(2, 'wind_speed', wind_speed, 'power_kw', power_kw) as (signal, value)"))


(generar_demo(df_turbinas().filter("cast(substring(turbine_id, 5) as int) % 4 = 0"), 1440)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("demo_telemetria")))
RUTA_JSON = f"{RUTA_VOL}/demo_dia2/json"
spark.table(t("demo_telemetria")).write.mode("overwrite").json(RUTA_JSON)
print(spark.table(t("demo_telemetria")).count(), "filas en demo_telemetria y su copia en", RUTA_JSON)

# Scope para la demo de secretos: el del lab 2a si ya existe y, si no, uno de prueba
from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()
_scopes = [s.name for s in _w.secrets.list_scopes()]
if "kv-formacion" in _scopes:
    SCOPE_DEMO = "kv-formacion"
else:
    SCOPE_DEMO = "demo-secretos"
    if SCOPE_DEMO not in _scopes:
        _w.secrets.create_scope(scope=SCOPE_DEMO)
    _w.secrets.put_secret(scope=SCOPE_DEMO, key="pg-password", string_value="valor-de-prueba")
print("Scope para la demo de secretos:", SCOPE_DEMO)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Un clúster es un driver que reparte trabajo

# COMMAND ----------

spark.range(0, 20_000_000).groupBy((F.col("id") % 10).alias("k")).count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué es un DataFrame de Spark

# COMMAND ----------

df = spark.table(t("demo_telemetria"))
print(type(df))
df.printSchema()
print("particiones:", df.select(F.spark_partition_id()).distinct().count())
try:
    df.rdd.getNumPartitions()
except Exception as e:
    print("df.rdd no funciona en serverless:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dónde se ejecuta cada línea

# COMMAND ----------

print(spark.table(t("demo_telemetria")).limit(3).collect())   # pocas filas: bien

t_ini = time.time()
filas = spark.range(0, 2_000_000).collect()
print(f"collect de 2 M filas: {time.time() - t_ini:.1f} s y {len(filas):,} objetos Row en la memoria del driver")
del filas

t_ini = time.time()
total = spark.range(0, 2_000_000).agg(F.sum("id")).first()[0]
print(f"sum en los executors: {time.time() - t_ini:.1f} s y al driver solo llega un número: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lazy evaluation: nada pasa hasta una acción

# COMMAND ----------

t_ini = time.time()
df2 = (spark.table(t("demo_telemetria")).filter(F.col("signal") == "power_kw")
       .groupBy("park_id").agg(F.avg("value").alias("media")))
print(f"transformaciones: {time.time() - t_ini:.2f} s")
t_ini = time.time()
df2.show()
print(f"acción: {time.time() - t_ini:.2f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalyst: el optimizador que reescribe tu código

# COMMAND ----------

(spark.table(t("demo_telemetria")).groupBy("park_id").agg(F.avg("value"))
      .filter("park_id = 'P03'")).explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer y escribir con la API de DataFrames

# COMMAND ----------

t_ini = time.time()
a = spark.read.json(RUTA_JSON)
a.printSchema()
print(f"inferido: {time.time() - t_ini:.1f} s")

t_ini = time.time()
b = spark.read.schema("turbine_id STRING, park_id STRING, ts TIMESTAMP, signal STRING, value DOUBLE").json(RUTA_JSON)
b.printSchema()
print(f"explícito: {time.time() - t_ini:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaciones del día a día

# COMMAND ----------

a = spark.createDataFrame([("WTG-0001", "P01")], "turbine_id string, park_id string")
b = spark.createDataFrame([("P02", "WTG-0002")], "park_id string, turbine_id string")   # mismas columnas, otro orden
display(a.union(b))         # junta por posición: P02 acaba en turbine_id y nadie avisa
display(a.unionByName(b))   # junta por nombre

# COMMAND ----------

crudo = spark.createDataFrame([("12.5",), ("N/A",), ("-9999",)], "v string")
display(crudo.select("v", F.expr("try_cast(v AS double)").alias("v_double")))   # N/A pasa a NULL en vez de fallar

# COMMAND ----------

def quitar_centinelas(df):
    return df.withColumn("value", F.when(F.col("value") != -9999, F.col("value")))


def a_mw(df):
    return df.withColumn("value_mw", F.col("value") / 1000)


lect = spark.createDataFrame([("WTG-0001", 2840.0), ("WTG-0002", -9999.0)], "turbine_id string, value double")
display(lect.transform(quitar_centinelas).transform(a_mw))   # funciones pequeñas que el viernes se pueden testear

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tipos de join que vais a usar

# COMMAND ----------

lect = spark.createDataFrame([("WTG-0001", 1.0), ("WTG-0999", 2.0)], "turbine_id string, value double")
maestro = df_turbinas()
display(lect.join(maestro, "turbine_id", "left_semi"))   # solo las que existen en el maestro
display(lect.join(maestro, "turbine_id", "left_anti"))   # las huérfanas

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark o SQL: el mismo motor

# COMMAND ----------

d = spark.table(t("demo_telemetria"))
d.filter("signal = 'power_kw'").groupBy("park_id").agg(F.avg("value")).explain()
spark.sql(f"SELECT park_id, avg(value) FROM {t('demo_telemetria')} WHERE signal = 'power_kw' GROUP BY park_id").explain()

# COMMAND ----------

display(spark.sql(f"SELECT park_id, count(*) AS filas FROM {t('demo_telemetria')} WHERE park_id = :parque GROUP BY park_id",
                  args={"parque": "P01"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaciones narrow y wide

# COMMAND ----------

d = spark.table(t("demo_telemetria"))
d.filter("signal = 'power_kw'").withColumn("mw", F.col("value") / 1000).explain()   # sin Exchange
d.groupBy("park_id").count().explain()                                                 # Exchange hashpartitioning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo ejecuta Spark un join

# COMMAND ----------

d = spark.table(t("demo_telemetria"))
m = df_turbinas()
d.join(m.hint("merge"), "turbine_id").explain()   # SortMergeJoin con un Exchange en cada lado
d.join(F.broadcast(m), "turbine_id").explain()    # BroadcastHashJoin: la tabla grande no se mueve

# COMMAND ----------

# MAGIC %md
# MAGIC ## Particiones: repartition y coalesce

# COMMAND ----------

n = lambda df: df.select(F.spark_partition_id()).distinct().count()
d = spark.table(t("demo_telemetria"))
print("leída:", n(d), " repartition(50):", n(d.repartition(50)), " coalesce(2):", n(d.repartition(50).coalesce(2)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caché: cuándo sí y cuándo no

# COMMAND ----------

try:
    spark.table(t("demo_telemetria")).cache().count()
    print("cache() aceptado en este cómputo")
except Exception as e:
    print("En serverless no hay caché de DataFrames:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data skew: cuando una tarea hace todo el trabajo

# COMMAND ----------

grande = (spark.range(0, 5_000_000)
          .withColumn("park_id", F.when(F.rand(1) < 0.7, "P01")
                      .otherwise(F.concat(F.lit("P0"), (F.rand(2) * 5 + 2).cast("int").cast("string")))))
tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")
display(grande.repartition(8, "park_id").groupBy(F.spark_partition_id().alias("particion")).count().orderBy("particion"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo se arregla el skew, en este orden

# COMMAND ----------

grande.join(F.broadcast(tarifas), "park_id").explain()   # BroadcastHashJoin, sin Exchange en el lado grande
N_SAL = 8
grande_sal = grande.withColumn("sal", (F.rand(4) * N_SAL).cast("int"))
display(grande_sal.repartition(8, "park_id", "sal").groupBy(F.spark_partition_id().alias("particion")).count().orderBy("particion"))   # reparto parejo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spill, Out of Memory y Spark UI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secretos: ninguna contraseña en el código

# COMMAND ----------

display(dbutils.secrets.listScopes())
display(dbutils.secrets.list(SCOPE_DEMO))
print(dbutils.secrets.get(SCOPE_DEMO, "pg-password"))   # sale como [REDACTED]
