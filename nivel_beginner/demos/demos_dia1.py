# Databricks notebook source
# MAGIC %md
# MAGIC # Día 1: La plataforma y primeros pasos
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

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


# Telemetría de ejemplo: 60 turbinas, un día, dos señales (172.800 filas)
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
# MAGIC Cada celda puede ser de Python, de SQL o de markdown, y el resultado sale debajo. Una celda de SQL empieza por `%sql`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quién soy, en qué catálogo y en qué esquema estoy: current_user(), current_catalog() y current_schema().

# COMMAND ----------

# MAGIC %md
# MAGIC Un widget es un parámetro que se cambia arriba del notebook sin tocar el código. `%run` ejecuta otro notebook y trae sus variables. `display` enseña un DataFrame como tabla, con pestañas para hacer gráficos y ver un perfil de los datos.

# COMMAND ----------

dbutils.widgets.text("parque", "P01")
parque = dbutils.widgets.get("parque")
print("Parque elegido:", parque)                          # cambiad el valor arriba y volved a ejecutar
print("Esta variable viene del %run de la configuración:", ESQUEMA)
display(spark.range(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Volumes y la primera tabla
# MAGIC Un volume es una carpeta de Unity Catalog para ficheros sueltos: CSV, JSON, imágenes... Su ruta es `/Volumes/catálogo/esquema/volume`. Una tabla es otra cosa: datos con columnas y tipos que se consultan con SQL. El paso típico es leer un fichero del volume y guardarlo como tabla.

# COMMAND ----------

# Lista lo que hay en la carpeta DEMO1 del volume con dbutils.fs.ls.

# COMMAND ----------

# Lee DEMO1/parques.csv en un DataFrame parques. La primera fila son los nombres de columna (header)
# y queremos que Spark deduzca los tipos (inferSchema). Enséñalo y enseña su esquema.

# COMMAND ----------

# Guarda parques como la tabla demo_parques (usa t("demo_parques") para el nombre completo).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Los parques de demo_parques cuyo operador es EnerSur.

# COMMAND ----------

# MAGIC %md
# MAGIC Las tablas que crea `saveAsTable` sin ruta son managed: Unity Catalog decide dónde van los ficheros y los gestiona. Si se borra una por error, durante 7 días se puede recuperar con `UNDROP`.

# COMMAND ----------

# Una copia con nombre único para enseñar DROP y UNDROP
copia = f"demo_drop_{int(time.time())}"
spark.table(t("demo_parques")).write.saveAsTable(t(copia))
spark.sql(f"DROP TABLE {t(copia)}")
display(spark.sql(f"SHOW TABLES DROPPED IN {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

spark.sql(f"UNDROP TABLE {t(copia)}")
display(spark.table(t(copia)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Formatos
# MAGIC Los mismos datos guardados en CSV, JSON, Parquet y Delta.
# MAGIC
# MAGIC - **CSV y JSON** son texto, fila a fila. Se leen con cualquier herramienta, pero ocupan mucho y el CSV no guarda los tipos.
# MAGIC - **Parquet** guarda por columnas, comprimido y con los tipos. Para leer dos columnas no hace falta leer el resto.
# MAGIC - **Delta** es Parquet más un registro de cambios. Ese registro da transacciones, historial y más cosas que veremos el miércoles.

# COMMAND ----------

df = spark.table(t("demo_telemetria"))
base = f"{DEMO1}/formatos"
display(df.limit(5))

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

# Lee otra vez el CSV de base/csv (con cabecera) y el Parquet de base/parquet, y enseña los dos esquemas.
