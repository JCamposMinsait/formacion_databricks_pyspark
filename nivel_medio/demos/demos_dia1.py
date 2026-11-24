# Databricks notebook source
# MAGIC %md
# MAGIC # Día 1: Plataforma, cómputo y Delta Lake

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="1"

# COMMAND ----------

# Datos para las demos, en EAV:
#   demo_telemetria        60 turbinas, 1 día (unas 173.000 filas). La usan casi todas las demos.
#   demo_telemetria_grande 240 turbinas, 3 días (unos 2 millones). Solo para la demo de Liquid Clustering.
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
(generar_demo(df_turbinas(), 3 * 1440)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("demo_telemetria_grande")))
print(spark.table(t("demo_telemetria")).count(), "filas en demo_telemetria")
print(spark.table(t("demo_telemetria_grande")).count(), "filas en demo_telemetria_grande")

# COMMAND ----------

# MAGIC %md
# MAGIC ## El notebook: lo que hay que dominar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS yo, current_catalog() AS catalogo, current_schema() AS esquema

# COMMAND ----------

dbutils.widgets.text("planta", "P01")
planta = dbutils.widgets.get("planta")
print("Planta elegida:", planta)                                  # cambiad el valor arriba y volved a ejecutar
print("Esta variable viene del %run de 00_config:", ESQUEMA)
display(spark.range(100))                                         # pestañas de tabla, visualización y Data Profile

# COMMAND ----------

dbutils.fs.help()   # toda la ayuda de ficheros sin salir del notebook

# COMMAND ----------

# Para el depurador: poned un punto de parada en la línea marcada y ejecutad la celda con Debug.
def potencia_teorica(v, rotor_m=150, nominal_kw=4200):
    area = math.pi * (rotor_m / 2) ** 2
    p = 0.5 * 1.225 * area * v ** 3 * 0.45 / 1000   # punto de parada aquí
    return min(p, nominal_kw)


print(potencia_teorica(9.0))

# COMMAND ----------

# MAGIC %md
# MAGIC ## De DBFS y mounts a Volumes

# COMMAND ----------

display(dbutils.fs.ls(RUTA_VOL))
with open(f"{RUTA_VOL}/hola.txt", "w") as f:
    f.write("Esto es un fichero en un Volume de Unity Catalog\n")
print(open(f"{RUTA_VOL}/hola.txt").read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog: todo tiene tres apellidos

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas managed y external

# COMMAND ----------

DEMO_DROP = f"demo_drop_{int(time.time())}"
spark.sql(f"CREATE TABLE {DEMO_DROP} AS SELECT 1 AS id")
spark.sql(f"DROP TABLE {DEMO_DROP}")
print("Tabla borrada:", DEMO_DROP)
display(spark.sql("SHOW TABLES DROPPED"))

# COMMAND ----------

spark.sql(f"UNDROP TABLE {DEMO_DROP}")
display(spark.table(DEMO_DROP))   # ha vuelto

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué formato usar para cada cosa

# COMMAND ----------

df = spark.table(t("demo_telemetria"))
base = f"{RUTA_VOL}/formatos"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué es Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo_fmt (id INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL demo_fmt   -- columna format = delta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Una tabla Delta es Parquet más un diario

# COMMAND ----------

RUTA_EXTERNA = ""   # en Free Edition, vacía: no hay external locations
if RUTA_EXTERNA:
    spark.sql("DROP TABLE IF EXISTS demo_log")
    dbutils.fs.rm(RUTA_EXTERNA, True)            # empezamos de cero en cada ensayo
    spark.sql(f"CREATE TABLE demo_log LOCATION '{RUTA_EXTERNA}' AS SELECT * FROM {t('demo_telemetria')} LIMIT 1000")
    spark.sql("UPDATE demo_log SET value = value * 2 WHERE signal = 'power_kw'")
    display(dbutils.fs.ls(f"{RUTA_EXTERNA}/_delta_log"))
    print(dbutils.fs.head(f"{RUTA_EXTERNA}/_delta_log/00000000000000000001.json", 3000))   # buscad "add" y "remove"
else:
    display(spark.sql(f"DESCRIBE DETAIL {t('demo_telemetria')}"))

# COMMAND ----------

# La pregunta trampa de la slide de managed y external: DROP de una external no borra los datos.
if RUTA_EXTERNA:
    spark.sql("DROP TABLE demo_log")
    print("Tras el DROP, los ficheros siguen en el storage:")
    display(dbutils.fs.ls(RUTA_EXTERNA))
    spark.sql(f"CREATE TABLE demo_log LOCATION '{RUTA_EXTERNA}'")
    display(spark.sql("DESCRIBE HISTORY demo_log"))   # vuelve con su historial

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escribir en Delta: qué comando para qué

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE demo_escritura (id INT, v STRING)")
for _ in range(2):
    spark.sql("INSERT INTO demo_escritura VALUES (1, 'a'), (2, 'b')")
print("Tras dos INSERT INTO:", spark.table(t("demo_escritura")).count(), "filas, duplicado")
for _ in range(2):
    spark.sql("INSERT OVERWRITE demo_escritura VALUES (1, 'a'), (2, 'b')")
print("Tras dos INSERT OVERWRITE:", spark.table(t("demo_escritura")).count(), "filas, idempotente")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel: cada commit es una versión

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS demo_tt")   # así la historia empieza siempre en la versión 0
spark.sql("CREATE TABLE demo_tt DEEP CLONE demo_telemetria")
display(spark.sql("SELECT park_id, count(*) AS filas FROM demo_tt GROUP BY ALL ORDER BY 1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM demo_tt WHERE park_id = 'P04'   -- el error

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT park_id, count(*) AS filas FROM demo_tt GROUP BY ALL ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo_tt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS filas_en_la_version_0 FROM demo_tt VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE demo_tt TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo_tt   -- el RESTORE es una versión nueva

# COMMAND ----------

# MAGIC %md
# MAGIC ## El problema de los ficheros pequeños

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS demo_small")
for i in range(40):
    spark.range(1000).write.mode("append").saveAsTable(t("demo_small"))
print("ficheros antes:", spark.sql("DESCRIBE DETAIL demo_small").first().numFiles)

# COMMAND ----------

display(spark.sql("OPTIMIZE demo_small"))
print("ficheros después:", spark.sql("DESCRIBE DETAIL demo_small").first().numFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ordenar los datos para leer menos

# COMMAND ----------

COLS = "turbine_id STRING, park_id STRING, ts TIMESTAMP, signal STRING, value DOUBLE"
spark.sql("DROP TABLE IF EXISTS demo_sin_cluster")
spark.sql("DROP TABLE IF EXISTS demo_liquid")

spark.sql(f"CREATE TABLE demo_sin_cluster ({COLS}) TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'false')")
spark.table(t("demo_telemetria_grande")).repartition(16).write.mode("append").saveAsTable(t("demo_sin_cluster"))

spark.sql(f"CREATE TABLE demo_liquid ({COLS}) CLUSTER BY (turbine_id) TBLPROPERTIES ('delta.targetFileSize' = '2mb')")
spark.table(t("demo_telemetria_grande")).write.mode("append").saveAsTable(t("demo_liquid"))
spark.sql("OPTIMIZE demo_liquid")

for tabla in ["demo_sin_cluster", "demo_liquid"]:
    total = spark.sql(f"DESCRIBE DETAIL {tabla}").first().numFiles
    con_datos = (spark.table(t(tabla)).filter("turbine_id = 'WTG-0040'")
                 .select(F.countDistinct("_metadata.file_path")).first()[0])
    print(f"{tabla:18s} WTG-0040 está en {con_datos} de {total} ficheros")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), avg(value) FROM demo_sin_cluster WHERE turbine_id = 'WTG-0040'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), avg(value) FROM demo_liquid WHERE turbine_id = 'WTG-0040'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL demo_liquid   -- columna clusteringColumns

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM y la retención

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM demo_tt DRY RUN   -- vacía o casi: todo tiene menos de 7 días

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES demo_tt

# COMMAND ----------

# Opcional, si hay permiso sobre las tablas de sistema: OPTIMIZE y VACUUM lanzados por Predictive Optimization.
try:
    display(spark.sql("SELECT * FROM system.storage.predictive_optimization_operations_history LIMIT 20"))
except Exception as e:
    print("Sin acceso a la tabla de sistema:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza
