# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1b · Ingesta masiva de telemetría → Bronze + Delta Lake (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~1 h 15 min
# MAGIC
# MAGIC Al terminar sabrás: cargar ficheros de un Volume a una tabla Bronze, ver cómo funciona Delta por dentro (log, ACID, esquema, time travel) y optimizar el layout de una tabla.
# MAGIC
# MAGIC > **Cómo usar esta guía**
# MAGIC > - Cada paso tiene un objetivo, la slide del curso donde está explicado y unas pistas.
# MAGIC > - Rellena lo marcado con `___` o `# TODO`. Si una celda no tiene nada que rellenar, léela y ejecútala.
# MAGIC > - Si te atascas: primero la slide, luego la documentación de Databricks y, como último recurso, la solución en `labs/soluciones/`.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Generar la telemetría cruda
# MAGIC **Objetivo:** tener ~12 M de lecturas JSON en el Volume, como las dejaría un concentrador.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · Telemetría de aerogeneradores
# MAGIC
# MAGIC **Pistas**
# MAGIC - No tienes que escribir nada: lee el código y ejecútalo.
# MAGIC - DIAS viene en 2 para no agotar la cuota diaria de cómputo de Free Edition.

# COMMAND ----------

import math, datetime as dt

DIAS   = 2
INICIO = dt.datetime(2026, 11, 2, 0, 0, 0, tzinfo=dt.timezone.utc)

turbinas = df_turbinas()
minutos  = spark.range(0, DIAS * 1440).withColumnRenamed("id", "m")

DEGRADADAS = ["WTG-0042", "WTG-0117", "WTG-0199"]   # palas sucias o desalineadas: menos Cp
CALIENTES  = ["WTG-0013", "WTG-0187"]               # multiplicadora con problemas

base = (turbinas.select("turbine_id", "park_id", "potencia_nominal_kw", "diametro_rotor_m")
        .crossJoin(minutos)
        .withColumn("ts", F.timestamp_seconds(F.lit(int(INICIO.timestamp())) + F.col("m") * 60))
        # viento: patrón diario + ruido + "carácter" de cada parque
        .withColumn("park_bias", F.when(F.col("park_id") == "P03", 1.8)
                                  .when(F.col("park_id") == "P06", 1.2)
                                  .when(F.col("park_id") == "P02", -1.0).otherwise(0.0))
        .withColumn("wind_speed", F.greatest(F.lit(0.0),
                     F.lit(5.8) + F.col("park_bias")
                     + F.lit(3.0) * F.sin(F.lit(2 * math.pi) * (F.col("m") % 1440) / 1440)
                     + F.randn(seed=1) * 1.8))
        # curva de potencia física: P = ½·ρ·A·v³·Cp, limitada a la nominal. Arranque 3 m/s, corte 25 m/s
        .withColumn("cp", F.when(F.col("turbine_id").isin(DEGRADADAS), 0.30).otherwise(0.40))
        .withColumn("power_kw", F.when((F.col("wind_speed") < 3) | (F.col("wind_speed") >= 25), F.lit(0.0))
                                 .otherwise(F.least(F.col("potencia_nominal_kw").cast("double"),
                                                    F.lit(0.5 * 1.225 * math.pi / 1000) * F.pow(F.col("diametro_rotor_m") / 2, 2)
                                                    * F.pow("wind_speed", 3) * F.col("cp")))
                    + F.randn(seed=2) * 40)
        .withColumn("rotor_rpm", F.least(F.lit(14.5), F.col("wind_speed") * 1.15) + F.randn(seed=3) * 0.2)
        .withColumn("temp_gearbox_c", F.lit(42.0) + F.col("power_kw") / F.col("potencia_nominal_kw") * 28 + F.randn(seed=4) * 1.5
                    + F.when(F.col("turbine_id").isin(CALIENTES), 8.0).otherwise(0.0))
        .withColumn("vibration_mm_s", F.abs(F.randn(seed=5)) * 0.6 + F.col("rotor_rpm") * 0.05)
)

# EAV: una fila por (turbina, instante, señal)
eav = (base.selectExpr(
          "turbine_id", "park_id", "ts",
          "stack(5, 'wind_speed', wind_speed, 'power_kw', power_kw, 'rotor_rpm', rotor_rpm, "
          "'temp_gearbox_c', temp_gearbox_c, 'vibration_mm_s', vibration_mm_s) as (signal, value)")
       # suciedad realista
       .withColumn("value", F.when(F.rand(seed=6) < 0.002, F.lit(-9999.0))          # centinela del PLC
                             .when(F.rand(seed=7) < 0.003, F.lit(None).cast("double"))  # nulos
                             .otherwise(F.round("value", 3)))
       .withColumn("quality", F.when(F.rand(seed=8) < 0.01, "BAD").otherwise("GOOD"))
       .withColumn("fw_version", F.when(F.col("park_id").isin("P01", "P02"), "3.2.1").otherwise("3.1.0"))
       .withColumn("ts", F.date_format("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
       .withColumn("fecha", F.substring("ts", 1, 10)))

duplicados = eav.sample(fraction=0.005, seed=9)   # el típico reenvío del concentrador
# Turbinas que no están en el maestro (una alta nueva que aún no han dado de alta en el ERP).
# Mañana acabarán en la tabla de cuarentena.
huerfanas = (eav.filter(F.col("turbine_id").isin("WTG-0001", "WTG-0002"))
             .withColumn("turbine_id", F.when(F.col("turbine_id") == "WTG-0001", "WTG-0901").otherwise("WTG-0902")))
crudo = eav.unionByName(duplicados).unionByName(huerfanas)

(crudo.repartition(48)
      .write.mode("overwrite")
      .partitionBy("fecha")
      .json(RUTA_RAW_BATCH))

print("Ficheros generados:")
display(dbutils.fs.ls(RUTA_RAW_BATCH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Explorar los ficheros sin cargarlos
# MAGIC **Objetivo:** mirar qué hay antes de crear ninguna tabla.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · Qué formato usar para cada cosa · Qué es Delta Lake
# MAGIC
# MAGIC **Pistas**
# MAGIC - read_files lee una carpeta y descubre el esquema y las particiones.
# MAGIC - count_if(condición) cuenta las filas que cumplen algo.

# COMMAND ----------

# TODO: muestra 20 filas de la carpeta RUTA_RAW_BATCH con read_files en formato json
display(spark.sql(f"""
  SELECT * FROM read_files('{RUTA_RAW_BATCH}', format => '___') LIMIT 20
"""))

# COMMAND ----------

# TODO: por cada señal (signal): número de filas, media de value, cuántas centinelas (-9999) y cuántos nulos
display(spark.sql(f"""
  SELECT signal,
         count(*)              AS filas,
         ___                   AS media,
         count_if(___)         AS centinelas,
         count_if(___)         AS nulos
  FROM read_files('{RUTA_RAW_BATCH}', format => 'json')
  GROUP BY signal
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Crear la tabla Bronze
# MAGIC **Objetivo:** cargar los ficheros tal cual llegan, con dos columnas de auditoría.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · Una tabla Delta es Parquet más un diario · Escribir en Delta: qué comando para qué
# MAGIC
# MAGIC **Pistas**
# MAGIC - Esquema explícito: nada de inferir en producción.
# MAGIC - La ruta del fichero de origen está en la columna oculta _metadata.file_path.
# MAGIC - La fecha de carga: F.current_timestamp().
# MAGIC - Escribe con saveAsTable(t("bronze_telemetria")).

# COMMAND ----------

esquema_raw = "turbine_id STRING, park_id STRING, ts STRING, signal STRING, value DOUBLE, quality STRING, fw_version STRING"

bronze = (spark.read.schema(esquema_raw).json(RUTA_RAW_BATCH)
          .select("*",
                  ___.alias("_source_file"),      # TODO
                  ___.alias("_ingested_at")))     # TODO

# Si relanzáis el lab, borramos antes la tabla para que su historia vuelva a empezar en la versión 0
# (las celdas de time travel de la sección 5 usan las versiones 0 y 2).
spark.sql(f"DROP TABLE IF EXISTS {t('bronze_telemetria')}")

# TODO: escribe en la tabla bronze_telemetria (modo overwrite)
bronze.write.___

print(spark.table(t("bronze_telemetria")).count(), "filas en bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: mira el detalle físico de la tabla (formato, ubicación, número de ficheros)
# MAGIC ___ bronze_telemetria;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: mira el historial de la tabla
# MAGIC ___ bronze_telemetria;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · ACID y control de esquema
# MAGIC **Objetivo:** ver que cada operación es un commit y que Delta protege el esquema.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · ACID, traducido a ficheros · Enforcement y evolution del esquema
# MAGIC
# MAGIC **Pistas**
# MAGIC - UPDATE y DELETE funcionan como en cualquier base de datos.
# MAGIC - Después de cada operación, vuelve a mirar el historial.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: el parque P02 mandó la potencia en W en vez de kW entre las 10:00 y las 10:59 del 2026-11-02.
# MAGIC -- Multiplica value por 1000 en esas filas (signal = 'power_kw', ts que empiece por '2026-11-02T10:')
# MAGIC UPDATE bronze_telemetria
# MAGIC SET ___
# MAGIC WHERE ___;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: borra las filas con quality = 'BAD' del parque P05
# MAGIC ___;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_telemetria;   -- ¿qué versiones hay ahora? ¿qué operationMetrics tiene cada una?

# COMMAND ----------

nuevo_lote = spark.createDataFrame(
    [("WTG-0001", "P01", "2026-11-09T00:00:00Z", "power_kw", 1200.0, "GOOD", "3.2.1", "2026-11-09", "manual", None, "EU-SOUTH")],
    "turbine_id string, park_id string, ts string, signal string, value double, quality string, fw_version string, fecha string, _source_file string, _ingested_at timestamp, region string")

# TODO: intenta añadir nuevo_lote a la tabla en modo append SIN opciones y captura el error.
try:
    ___
except Exception as e:
    print("Rechazado:", str(e)[:300])

# COMMAND ----------

# TODO: ahora repite el append permitiendo que el esquema evolucione
nuevo_lote.write.mode("append").option(___, ___).saveAsTable(t("bronze_telemetria"))
spark.table(t("bronze_telemetria")).printSchema()   # ¿aparece la columna region?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Time travel y RESTORE
# MAGIC **Objetivo:** consultar versiones anteriores y deshacer cambios.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · Time travel: cada commit es una versión
# MAGIC
# MAGIC **Pistas**
# MAGIC - VERSION AS OF n o TIMESTAMP AS OF 'fecha'.
# MAGIC - EXCEPT ALL devuelve las filas que están en la primera consulta y no en la segunda.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: compara el número de filas de la versión 0 con el de la versión actual
# MAGIC SELECT 'v0' AS version, count(*) AS filas FROM bronze_telemetria ___
# MAGIC UNION ALL
# MAGIC SELECT 'actual', count(*) FROM bronze_telemetria;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: ¿qué filas borró el DELETE? Compara la versión 0 con la versión 2
# MAGIC SELECT * FROM bronze_telemetria VERSION AS OF ___
# MAGIC EXCEPT ALL
# MAGIC SELECT * FROM bronze_telemetria VERSION AS OF ___
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: deshaz todo volviendo a la versión 0 y mira el historial después
# MAGIC ___;
# MAGIC DESCRIBE HISTORY bronze_telemetria;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Layout: OPTIMIZE, Z-ORDER y Liquid Clustering
# MAGIC **Objetivo:** medir cuántos ficheros lee una consulta según cómo estén ordenados los datos.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · El problema de los ficheros pequeños · Ordenar los datos para leer menos
# MAGIC
# MAGIC **Pistas**
# MAGIC - Crea dos copias de la tabla: una con Z-ORDER y otra con Liquid Clustering, las dos por (turbine_id, ts).
# MAGIC - Compara files read y files pruned en el Query Profile (enlace See performance bajo el resultado).
# MAGIC - A las copias les ponemos un tamaño de fichero objetivo pequeño (32 MB) para que tengan bastantes ficheros y se note la diferencia. En producción esto no se toca.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: copia con Z-ORDER
# MAGIC CREATE OR REPLACE TABLE bronze_telemetria_zorder TBLPROPERTIES ('delta.targetFileSize' = '32mb') AS SELECT * FROM bronze_telemetria;
# MAGIC OPTIMIZE bronze_telemetria_zorder ___;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: copia con Liquid Clustering
# MAGIC CREATE OR REPLACE TABLE bronze_telemetria_liquid
# MAGIC ___
# MAGIC TBLPROPERTIES ('delta.targetFileSize' = '32mb')
# MAGIC AS SELECT * FROM bronze_telemetria;
# MAGIC OPTIMIZE bronze_telemetria_liquid;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lanza la misma consulta sobre las tres tablas, en celdas separadas, y compara el Query Profile
# MAGIC SELECT count(*), avg(value) FROM bronze_telemetria WHERE turbine_id = 'WTG-0200' AND signal = 'power_kw';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), avg(value) FROM bronze_telemetria_zorder WHERE turbine_id = 'WTG-0200' AND signal = 'power_kw';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), avg(value) FROM bronze_telemetria_liquid WHERE turbine_id = 'WTG-0200' AND signal = 'power_kw';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: cambia las columnas de clustering de la tabla liquid a (park_id, signal) sin reescribirla
# MAGIC ___;
# MAGIC DESCRIBE DETAIL bronze_telemetria_liquid;   -- mira clusteringColumns

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · VACUUM
# MAGIC **Objetivo:** ver qué ficheros borraría VACUUM sin borrarlos.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · VACUUM y la retención

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO
# MAGIC VACUUM bronze_telemetria ___;

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta:** ¿por qué no borra nada? ¿Qué pasaría con el time travel si VACUUM borrase los ficheros de la versión 0?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Managed y external
# MAGIC
# MAGIC > En Free Edition no se pueden crear external locations, así que la parte external se queda en la teoría. Haced la parte managed.
# MAGIC
# MAGIC **Objetivo:** ver de qué tipo son tus tablas.
# MAGIC
# MAGIC **Slide de referencia:** Día 1 · Tablas managed y external

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: lista nombre, tipo y formato de las tablas de tu esquema usando system.information_schema.tables
# MAGIC SELECT table_name, ___, data_source_format
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog = current_catalog() AND table_schema = ___;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. ¿Cuántas filas duplicadas exactas hay en Bronze? (pista: `GROUP BY ALL HAVING count(*) > 1`).
# MAGIC 2. Consulta la tabla tal y como estaba al crearla con `TIMESTAMP AS OF`. La hora de la versión 0 sale en `DESCRIBE HISTORY`.
# MAGIC 3. Crea `bronze_telemetria_auto` con `CLUSTER BY AUTO` y mira `DESCRIBE DETAIL`.
# MAGIC 4. Activa el Change Data Feed en Bronze, haz un `UPDATE` y léelo con `table_changes()`. Ojo: desde qué versión.
# MAGIC 5. (Examen) ¿Qué pasa si haces `VACUUM` con `RETAIN 0 HOURS` y luego intentas `VERSION AS OF 1`?
# MAGIC
# MAGIC ## Reto extra · Recuperación quirúrgica
# MAGIC Alguien ha borrado el parque P05 y después han seguido entrando datos. Recupera **solo P05** desde la versión buena, sin `RESTORE` de toda la tabla.
# MAGIC Día 1 · Time travel: cada commit es una versión

# COMMAND ----------

# Preparación del desastre (ejecutar tal cual)
version_buena = spark.sql("DESCRIBE HISTORY bronze_telemetria").agg(F.max("version")).first()[0]
spark.sql("DELETE FROM bronze_telemetria WHERE park_id = 'P05'")
spark.sql("""INSERT INTO bronze_telemetria
             SELECT * FROM bronze_telemetria WHERE park_id = 'P01' AND ts LIKE '2026-11-02T00:0%' LIMIT 100""")
print("Versión buena:", version_buena)

# COMMAND ----------

# TODO: lee P05 de la versión buena y añádelo a la tabla actual
p05_bueno = (spark.read.option(___, version_buena).table(t("bronze_telemetria"))
             .filter(___))
p05_bueno.write.mode(___).saveAsTable(t("bronze_telemetria"))
display(spark.sql("SELECT park_id, count(*) FROM bronze_telemetria GROUP BY ALL ORDER BY 1"))
