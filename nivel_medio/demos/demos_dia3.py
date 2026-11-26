# Databricks notebook source
# MAGIC %md
# MAGIC # Día 3: Medallion, SQL avanzado y UDFs
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="3"

# COMMAND ----------

# Datos para las demos: 60 turbinas, 1 día, en formato largo (172.800 filas).
import math, time, datetime as dt
import pandas as pd


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
print(spark.table(t("demo_telemetria")).count(), "filas en demo_telemetria")


def cronometrar(nombre, df):
    """Hace un count de df y dice cuánto ha tardado."""
    t0 = time.time()
    df.count()
    print(f"{nombre}: {time.time() - t0:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos anidados: struct, array y explode
# MAGIC Muchos mensajes de IoT llegan anidados: un `struct` es un grupo de campos con nombre (se accede con un punto) y un `array` es una lista. `explode` saca una fila por cada elemento de la lista.

# COMMAND ----------

msg = spark.createDataFrame([
    ("WTG-0001", "2026-11-02T10:00:00Z", [("power_kw", 2840.0), ("wind_speed", 9.2)], ("3.2.1", "P01")),
    ("WTG-0002", "2026-11-02T10:00:00Z", [], ("3.2.1", "P01"))],
    "turbine_id string, ts string, lecturas array<struct<signal:string,value:double>>, meta struct<fw:string,site:string>")
msg.printSchema()
display(msg)

# COMMAND ----------

# Saca una fila por lectura: turbine_id, meta.fw y explode de lecturas con alias l.
# Después despliega los campos de l con l.*.
# Repítelo con explode_outer y compara qué pasa con WTG-0002.

# COMMAND ----------

# MAGIC %md
# MAGIC ## PIVOT en SQL y en PySpark
# MAGIC La telemetría viene en formato largo, una fila por señal. Para comparar señales del mismo minuto hace falta una columna por señal. `PIVOT` hace ese giro.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sobre demo_telemetria, una fila por turbine_id y ts con una columna wind_speed y otra power_kw (avg de value).
# MAGIC -- Ordena por turbina y hora y enseña 10 filas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estrella o copo de nieve
# MAGIC Una tabla de hechos con lo que se mide (energía por turbina y día) y una dimensión con todo lo que describe a la turbina (parque, provincia, fabricante), ya aplanada. En estrella cada consulta es un solo join. En copo de nieve la dimensión se parte en varias tablas y hacen falta más joins.

# COMMAND ----------

dim = (df_turbinas().join(df_parques().select("park_id", F.col("nombre").alias("parque"), "provincia", "operador"), "park_id")
       .withColumn("fabricante", F.when(F.col("modelo").startswith("AE"), "AeroNorte")
                                  .when(F.col("modelo").startswith("BX"), "BoreasX")
                                  .otherwise("CierzoWind")))
dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("demo_dim_turbina"))
(spark.table(t("demo_telemetria")).filter("signal = 'power_kw'")
   .groupBy("turbine_id", F.to_date("ts").alias("fecha")).agg((F.sum("value") / 60 / 1000).alias("energia_mwh"))
   .write.mode("overwrite").saveAsTable(t("demo_hechos_energia")))
display(spark.table(t("demo_hechos_energia")).limit(5))
display(spark.table(t("demo_dim_turbina")).limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Energía total por fabricante y provincia: une demo_hechos_energia con demo_dim_turbina por turbine_id.
# MAGIC -- Redondea a 1 decimal y ordena de más a menos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas, vistas y vistas materializadas
# MAGIC - **Tabla**: los datos guardados. Hay que recargarla para que cambie.
# MAGIC - **Vista**: una consulta con nombre. No guarda nada y se calcula cada vez que se lee, así que siempre está al día pero cuesta lo que cueste la consulta.
# MAGIC - **Vista materializada**: una consulta cuyo resultado se guarda y Databricks refresca. Se lee rápido y se mantiene sola.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crea la vista demo_v_energia_parque con la energía total por parque (hechos y dimensión por turbine_id).
# MAGIC -- Después consúltala.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW demo_mv_energia_parque AS SELECT * FROM demo_v_energia_parque

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_mv_energia_parque

# COMMAND ----------

# MAGIC %md
# MAGIC ## NULL: la fuente de la mitad de los bugs
# MAGIC Antes de ejecutar, ¿qué sale en cada columna?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NULL = NULL             AS igual,
# MAGIC        NULL <=> NULL           AS igual_null_safe,
# MAGIC        NULL + 5                AS suma,
# MAGIC        10 / NULLIF(0, 0)       AS division_segura,
# MAGIC        coalesce(NULL, NULL, 0) AS coalesce_

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS filas, count(v) AS no_nulos, avg(v) AS media_ignora_nulos, avg(coalesce(v, 0)) AS media_nulo_como_cero
# MAGIC FROM VALUES (10), (NULL), (20) AS t(v)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funciones de ventana: mirar a las filas vecinas
# MAGIC Una función de ventana calcula algo para cada fila mirando a otras filas, sin agrupar: cada fila sigue ahí. `PARTITION BY` dice qué filas son vecinas, `ORDER BY` en qué orden y el marco (`ROWS` o `RANGE`) cuántas se miran.
# MAGIC
# MAGIC El marco por defecto despista: con `ORDER BY` y sin marco, la ventana va desde el principio hasta la fila actual.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v,
# MAGIC        avg(v) OVER (ORDER BY v)                                          AS sin_marco_acumulada,
# MAGIC        avg(v) OVER ()                                                    AS de_todo,
# MAGIC        avg(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ultimas_2
# MAGIC FROM VALUES (10), (20), (30), (40) AS t(v)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAG y LEAD en series temporales
# MAGIC `LAG` trae el valor de la fila anterior y `LEAD` el de la siguiente. Con la hora anterior se ven los huecos: minutos en los que una turbina no mandó nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_id, ts, LAG(ts) OVER w AS anterior, timestampdiff(MINUTE, LAG(ts) OVER w, ts) AS min_desde_anterior
# MAGIC FROM VALUES ('WTG-1', timestamp'2026-11-02 10:00:00'), ('WTG-1', timestamp'2026-11-02 10:01:00'),
# MAGIC             ('WTG-1', timestamp'2026-11-02 10:07:00'), ('WTG-1', timestamp'2026-11-02 10:08:00') AS t(turbine_id, ts)
# MAGIC WINDOW w AS (PARTITION BY turbine_id ORDER BY ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medias móviles: ROWS o RANGE
# MAGIC `ROWS` cuenta filas y `RANGE` cuenta tiempo. Con huecos en los datos no dan lo mismo.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ts, temp,
# MAGIC        avg(temp) OVER (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)                   AS rows_3_filas,
# MAGIC        avg(temp) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 3 MINUTES PRECEDING AND CURRENT ROW) AS range_3_min
# MAGIC FROM VALUES (timestamp'2026-11-02 10:00:00', 50.0), (timestamp'2026-11-02 10:01:00', 52.0),
# MAGIC             (timestamp'2026-11-02 10:07:00', 80.0), (timestamp'2026-11-02 10:08:00', 82.0) AS t(ts, temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicar con row_number y QUALIFY
# MAGIC Cuando la misma lectura llega dos veces con valores distintos, `dropDuplicates` se queda con una cualquiera. Con `row_number` se decide cuál: por ejemplo, la última que llegó. `QUALIFY` filtra por el resultado de una ventana, como `HAVING` filtra por un agregado.

# COMMAND ----------

spark.createDataFrame([
    ("WTG-1", dt.datetime(2026, 11, 2, 10, 0, 0), "power_kw", 100.0, dt.datetime(2026, 11, 2, 10, 0, 5)),
    ("WTG-1", dt.datetime(2026, 11, 2, 10, 0, 0), "power_kw", 120.0, dt.datetime(2026, 11, 2, 10, 3, 0)),
    ("WTG-1", dt.datetime(2026, 11, 2, 10, 1, 0), "power_kw", 130.0, dt.datetime(2026, 11, 2, 10, 1, 4))],
    "turbine_id string, ts timestamp, signal string, value double, ingested_at timestamp").createOrReplaceTempView("ingestas_v")
display(spark.table("ingestas_v"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- De ingestas_v, una fila por turbine_id, ts y signal: la que llegó la última (ingested_at más reciente).
# MAGIC -- Usa row_number() en una ventana y QUALIFY.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v, row_number() OVER (ORDER BY v) AS rn, rank() OVER (ORDER BY v) AS rk, dense_rank() OVER (ORDER BY v) AS drk
# MAGIC FROM VALUES (10), (20), (20), (30) AS t(v)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fechas, franjas y zonas horarias
# MAGIC Las turbinas mandan la hora en UTC y se guarda en UTC. Se pasa a hora local solo para enseñarla. En el cambio de hora de octubre, la misma hora local pasa dos veces.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET TIME ZONE 'UTC'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ts_utc, from_utc_timestamp(ts_utc, 'Europe/Madrid') AS madrid
# MAGIC FROM VALUES (timestamp'2026-10-25 00:30:00'), (timestamp'2026-10-25 01:30:00') AS t(ts_utc)   -- las dos son las 02:30 en Madrid

# COMMAND ----------

# MAGIC %md
# MAGIC ## UDFs de Python: potentes y lentas
# MAGIC ## Pandas UDFs: vectorizadas con Arrow
# MAGIC - Una **UDF** normal recibe un valor cada vez: cada fila sale del motor a Python y vuelve.
# MAGIC - Una **Pandas UDF** recibe un bloque de miles de filas como `pd.Series`, que viaja en formato Arrow, y trabaja con pandas sobre el bloque entero.
# MAGIC - Una **función nativa** (`F.when`, `F.pow`...) no sale del motor.
# MAGIC
# MAGIC La misma potencia teórica de las tres formas sobre 2 millones de velocidades de viento.

# COMMAND ----------

from pyspark.sql.functions import udf, pandas_udf

K = 0.5 * 1.225 * math.pi * 75 ** 2 * 0.45 / 1000   # kW por (m/s)^3 con un rotor de 150 m


@udf("double")
def con_udf(v):
    return 0.0 if v < 3 else min(K * v ** 3, 4200.0)


@pandas_udf("double")
def con_pandas_udf(v: pd.Series) -> pd.Series:
    return (K * v ** 3).clip(upper=4200.0).where(v >= 3, 0.0)


nativa = F.when(F.col("v") < 3, 0.0).otherwise(F.least(F.lit(K) * F.pow("v", 3), F.lit(4200.0)))

datos = spark.range(2_000_000).select((F.rand(1) * 20).alias("v"))
cronometrar("UDF       ", datos.select(con_udf("v").alias("p")).filter("p > 1000"))
cronometrar("Pandas UDF", datos.select(con_pandas_udf("v").alias("p")).filter("p > 1000"))
cronometrar("Nativa    ", datos.select(nativa.alias("p")).filter("p > 1000"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orden de preferencia para extender Spark
# MAGIC 1. Funciones nativas de Spark.
# MAGIC 1. Funciones SQL de Unity Catalog: se escriben en SQL, quedan guardadas en el catálogo con permisos y las usa cualquiera, desde SQL o desde Python.
# MAGIC 1. Pandas UDF.
# MAGIC 1. UDF de Python, solo si no hay otra forma.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crea la función demo_fn_mwh(kw DOUBLE) que devuelve kw / 60 / 1000 (kW de un minuto a MWh), con un comentario.
# MAGIC -- Pruébala con 3000.

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE INTO: insertar, actualizar y borrar a la vez
# MAGIC `MERGE` cruza la tabla destino con el origen por una clave. `WHEN MATCHED` es lo que está en los dos, `WHEN NOT MATCHED` lo que solo está en el origen y `WHEN NOT MATCHED BY SOURCE` lo que solo está en el destino. Cada caso puede actualizar, insertar o borrar, y se le pueden añadir condiciones.

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE demo_merge AS SELECT * FROM VALUES ('A', 1), ('B', 2), ('C', 3) AS t(k, v)")
spark.sql("CREATE OR REPLACE TEMP VIEW origen_v AS SELECT * FROM VALUES ('B', 20), ('D', 4) AS s(k, v)")
display(spark.table("demo_merge").orderBy("k"))
display(spark.table("origen_v").orderBy("k"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE de origen_v en demo_merge por k:
# MAGIC -- si está en los dos, actualiza v; si solo está en el origen, insértala;
# MAGIC -- si solo está en el destino y k es 'C', bórrala.
# MAGIC -- Después enseña demo_merge ordenada por k.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD tipo 2 con un único MERGE
# MAGIC En un SCD tipo 2 no se pisa el valor viejo: se cierra la fila vigente (fecha de fin y `es_actual = false`) y se inserta una nueva con el valor nuevo. Así se sabe qué estado tenía cada turbina en cada momento.
# MAGIC
# MAGIC El problema es que un `MERGE` hace una sola cosa por cada fila del origen, y aquí hacen falta dos (cerrar e insertar). El truco es meter cada cambio dos veces en el origen: una con la clave, que encuentra la fila vigente y la cierra, y otra con la clave a `NULL`, que no encuentra nada y se inserta.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS demo_scd2")
spark.sql("""
CREATE TABLE demo_scd2 (
  sk_turbina   BIGINT GENERATED ALWAYS AS IDENTITY,
  turbine_id   STRING,
  estado       STRING,
  valido_desde TIMESTAMP,
  valido_hasta TIMESTAMP,
  es_actual    BOOLEAN)
""")
spark.sql("""
INSERT INTO demo_scd2 (turbine_id, estado, valido_desde, valido_hasta, es_actual)
VALUES ('WTG-0007', 'OPERATIVA', timestamp'2024-01-01', NULL, true)
""")
spark.sql("CREATE OR REPLACE TEMP VIEW extracto AS SELECT * FROM VALUES ('WTG-0007', 'MANTENIMIENTO') AS e(turbine_id, estado)")
display(spark.table("demo_scd2"))
display(spark.table("extracto"))

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo_scd2 d
# MAGIC USING (
# MAGIC   SELECT turbine_id AS merge_key, * FROM extracto
# MAGIC   UNION ALL
# MAGIC   SELECT NULL, e.* FROM extracto e JOIN demo_scd2 d
# MAGIC     ON e.turbine_id = d.turbine_id AND d.es_actual AND e.estado <> d.estado
# MAGIC ) s
# MAGIC ON d.turbine_id = s.merge_key AND d.es_actual
# MAGIC WHEN MATCHED AND d.estado <> s.estado THEN
# MAGIC   UPDATE SET es_actual = false, valido_hasta = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (turbine_id, estado, valido_desde, valido_hasta, es_actual)
# MAGIC   VALUES (s.turbine_id, s.estado, current_timestamp(), NULL, true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_scd2 ORDER BY sk_turbina   -- dos versiones, cada una con su clave subrogada

# COMMAND ----------

# MAGIC %md
# MAGIC ## CLONE: copias de tablas en segundos
# MAGIC - `DEEP CLONE` copia los datos y el esquema a una tabla independiente. Sirve para copias de seguridad o para llevar datos a otro entorno.
# MAGIC - `SHALLOW CLONE` solo copia el diario: apunta a los ficheros de la original. Es instantáneo y sirve para pruebas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Haz una copia completa de demo_scd2 llamada demo_scd2_backup y mira su historial.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentar las tablas: el metadato también cuenta
# MAGIC Comentarios en tablas y columnas, claves primarias y ajenas (informativas: Databricks no las comprueba, pero las usan las herramientas y el optimizador) y etiquetas para buscar y gobernar.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE demo_hechos_energia IS 'Energía diaria por turbina (MWh)';
# MAGIC ALTER TABLE demo_hechos_energia ALTER COLUMN energia_mwh COMMENT 'Suma de la potencia media de cada minuto / 60 / 1000';
# MAGIC ALTER TABLE demo_hechos_energia DROP CONSTRAINT IF EXISTS fk_demo_turbina;
# MAGIC ALTER TABLE demo_dim_turbina DROP PRIMARY KEY IF EXISTS;
# MAGIC ALTER TABLE demo_dim_turbina ALTER COLUMN turbine_id SET NOT NULL;
# MAGIC ALTER TABLE demo_dim_turbina ADD CONSTRAINT pk_demo_turbina PRIMARY KEY (turbine_id);
# MAGIC ALTER TABLE demo_hechos_energia ADD CONSTRAINT fk_demo_turbina FOREIGN KEY (turbine_id) REFERENCES demo_dim_turbina;
# MAGIC ALTER TABLE demo_hechos_energia SET TAGS ('dominio' = 'eolica', 'capa' = 'gold');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED demo_hechos_energia
