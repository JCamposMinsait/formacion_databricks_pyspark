# Databricks notebook source
# MAGIC %md
# MAGIC # Día 1: Plataforma, cómputo y Delta Lake
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="1"

# COMMAND ----------

# Datos para las demos, en formato largo (una fila por turbina, minuto y señal):
#   demo_telemetria        60 turbinas, 1 día (172.800 filas). La usan casi todas las demos.
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
# MAGIC Celdas de Python, SQL (`%sql`) y markdown en el mismo notebook. Widgets para parametrizar, `%run` para traer configuración de otro notebook, `dbutils` para ficheros, secretos y widgets, y el depurador para parar dentro de una función.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quién soy, en qué catálogo y en qué esquema estoy: current_user(), current_catalog() y current_schema().

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
# MAGIC Antes los ficheros iban a DBFS o a mounts de un storage, sin permisos por usuario. Los Volumes los sustituyen: son objetos de Unity Catalog, con permisos como una tabla, y se usan como una carpeta normal, también desde Python con `open()`.

# COMMAND ----------

# Lista el contenido de RUTA_VOL.
# Escribe un hola.txt en el volume con open() de Python y léelo otra vez.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog: todo tiene tres apellidos
# MAGIC `catálogo.esquema.objeto`. El catálogo suele separar entornos o áreas, el esquema agrupa lo de un tema y el objeto es la tabla, vista, volume o función. Los permisos se dan en cualquier nivel y se heredan hacia abajo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Los catálogos que veo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Las tablas del esquema en el que estoy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas managed y external
# MAGIC - **Managed**: Unity Catalog decide dónde van los ficheros y los gestiona. `DROP` borra los datos, pero durante 7 días se recuperan con `UNDROP`.
# MAGIC - **External**: la tabla apunta a una ruta de un storage que gestionas tú. `DROP` borra solo la definición y los ficheros se quedan.

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
# MAGIC - **CSV y JSON**: texto, fila a fila. Para intercambiar con otros sistemas. Ocupan mucho y el CSV no guarda tipos.
# MAGIC - **Parquet**: por columnas, comprimido y con tipos. Leer dos columnas no obliga a leer el resto.
# MAGIC - **Delta**: Parquet más un diario de cambios. Es el formato de las tablas en Databricks.

# COMMAND ----------

df = spark.table(t("demo_telemetria"))
base = f"{RUTA_VOL}/formatos"

# COMMAND ----------

# Escribe df en CSV (con cabecera) en base/csv, en JSON en base/json y en Parquet en base/parquet.
# Guárdalo también como la tabla Delta demo_formatos_delta.

# COMMAND ----------

def mb(ruta):
    """Megas que ocupan los ficheros de datos de una carpeta."""
    return sum(f.size for f in dbutils.fs.ls(ruta) if f.name.startswith("part-")) / 1024 / 1024


for fmt in ["csv", "json", "parquet"]:
    print(f"{fmt:8s} {mb(f'{base}/{fmt}'):6.1f} MB")
delta_mb = spark.sql(f"DESCRIBE DETAIL {t('demo_formatos_delta')}").first().sizeInBytes / 1024 / 1024
print(f"delta    {delta_mb:6.1f} MB")

# COMMAND ----------

# Lee otra vez el CSV de base/csv (con cabecera) y enseña su esquema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué es Delta Lake
# MAGIC Un formato abierto: ficheros Parquet más una carpeta `_delta_log` con un JSON por cada commit. Ese diario da transacciones ACID, historial, control de esquema y `UPDATE`, `DELETE` y `MERGE` sobre ficheros que en sí no se pueden modificar. En Databricks es el formato por defecto.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crea la tabla demo_fmt con una columna id INT, sin decir formato.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mira los detalles de demo_fmt (DESCRIBE DETAIL) y busca la columna format.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Una tabla Delta es Parquet más un diario
# MAGIC Cada commit añade un JSON al `_delta_log` con los ficheros que entran (`add`) y los que salen (`remove`). Un `UPDATE` no toca ficheros: escribe ficheros nuevos y marca los viejos como borrados.
# MAGIC
# MAGIC La carpeta de una tabla managed no se puede listar. Con una ruta externa en `RUTA_EXTERNA` se ve el diario entero. Sin ella (Free Edition), `DESCRIBE DETAIL` enseña lo que hay.

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
# MAGIC - `INSERT INTO` añade. Si la carga se repite, se duplica.
# MAGIC - `INSERT OVERWRITE` sustituye el contenido. Repetirlo deja lo mismo: es idempotente.
# MAGIC - `MERGE` actualiza lo que ya existe e inserta lo nuevo. Lo vemos el miércoles.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo_escritura (id INT, v STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inserta (1, 'a') y (2, 'b') con INSERT INTO y cuenta las filas. Ejecuta la celda dos veces.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lo mismo con INSERT OVERWRITE. Ejecuta la celda dos veces.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time travel: cada commit es una versión
# MAGIC Cada operación sobre una tabla Delta es un commit y deja una versión nueva. Una versión antigua se consulta con `VERSION AS OF` o `TIMESTAMP AS OF`, y se vuelve a ella con `RESTORE`. Vamos a borrar un parque por error y recuperarlo.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS demo_tt")   # así la historia empieza siempre en la versión 0
spark.sql("CREATE TABLE demo_tt DEEP CLONE demo_telemetria")
display(spark.sql("SELECT park_id, count(*) AS filas FROM demo_tt GROUP BY ALL ORDER BY 1"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El error: borra de demo_tt las filas del parque P04.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT park_id, count(*) AS filas FROM demo_tt GROUP BY ALL ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- El historial de demo_tt.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cuántas filas tenía demo_tt en la versión 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vuelve demo_tt a la versión 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo_tt   -- el RESTORE es una versión nueva

# COMMAND ----------

# MAGIC %md
# MAGIC ## El problema de los ficheros pequeños
# MAGIC Cada escritura deja al menos un fichero. Muchas escrituras pequeñas, como cargas cada minuto o streaming, dejan miles de ficheros diminutos, y abrirlos cuesta más que leerlos. `OPTIMIZE` los junta en ficheros grandes. Es un commit más: los viejos siguen ahí hasta el `VACUUM`.

# COMMAND ----------

# 40 escrituras de 1000 filas cada una
spark.sql("DROP TABLE IF EXISTS demo_small")
for i in range(40):
    spark.range(1000).write.mode("append").saveAsTable(t("demo_small"))
print("ficheros antes:", spark.sql("DESCRIBE DETAIL demo_small").first().numFiles)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compacta demo_small.

# COMMAND ----------

print("ficheros después:", spark.sql("DESCRIBE DETAIL demo_small").first().numFiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ordenar los datos para leer menos
# MAGIC Si las filas de una turbina están repartidas por todos los ficheros, filtrar por ella obliga a leerlos todos. Delta guarda en el diario el mínimo y el máximo de cada columna en cada fichero, y al filtrar se salta los ficheros que no pueden tener el valor (data skipping). Con `CLUSTER BY` (Liquid Clustering), Delta agrupa las filas por esa columna para que el salto sea grande. Sustituye a las particiones por carpetas y al `ZORDER`.
# MAGIC
# MAGIC La misma tabla de 2 millones de filas, sin ordenar y con `CLUSTER BY (turbine_id)`.

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

# MAGIC %md
# MAGIC La misma consulta en las dos tablas. Debajo de cada resultado, **See performance** enseña cuántos ficheros y bytes ha leído.

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
# MAGIC `VACUUM` borra del storage los ficheros que ya no usa la versión actual y que son más antiguos que la retención (7 días por defecto). Después ya no se puede volver a esas versiones. `DRY RUN` enseña lo que borraría sin borrar nada. En Unity Catalog, Predictive Optimization lanza `OPTIMIZE` y `VACUUM` solo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Qué borraría VACUUM en demo_tt, sin borrar nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES demo_tt

# COMMAND ----------

# Opcional, si hay permiso sobre las tablas de sistema: OPTIMIZE y VACUUM lanzados por Predictive Optimization.
try:
    display(spark.sql("SELECT * FROM system.storage.predictive_optimization_operations_history LIMIT 20"))
except Exception as e:
    print("Sin acceso a la tabla de sistema:", str(e).splitlines()[0][:150])
