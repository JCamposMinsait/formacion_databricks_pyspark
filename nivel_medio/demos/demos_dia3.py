# Databricks notebook source
# MAGIC %md
# MAGIC # Día 3: Medallion, SQL avanzado y UDFs

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="3"

# COMMAND ----------

# Datos para las demos: 60 turbinas, 1 día, en EAV (unas 173.000 filas).
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos anidados: struct, array y explode

# COMMAND ----------

msg = spark.createDataFrame([
    ("WTG-0001", "2026-11-02T10:00:00Z", [("power_kw", 2840.0), ("wind_speed", 9.2)], ("3.2.1", "P01")),
    ("WTG-0002", "2026-11-02T10:00:00Z", [], ("3.2.1", "P01"))],
    "turbine_id string, ts string, lecturas array<struct<signal:string,value:double>>, meta struct<fw:string,site:string>")
display(msg.select("turbine_id", "meta.fw", F.explode("lecturas").alias("l")).select("turbine_id", "fw", "l.*"))         # WTG-0002 desaparece
display(msg.select("turbine_id", "meta.fw", F.explode_outer("lecturas").alias("l")).select("turbine_id", "fw", "l.*"))   # se queda con nulos

# COMMAND ----------

# MAGIC %md
# MAGIC ## PIVOT en SQL y en PySpark

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (SELECT turbine_id, ts, signal, value FROM demo_telemetria)
# MAGIC PIVOT (avg(value) FOR signal IN ('wind_speed' AS wind_speed, 'power_kw' AS power_kw))
# MAGIC ORDER BY turbine_id, ts
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estrella o copo de nieve

# COMMAND ----------

dim = (df_turbinas().join(df_parques().select("park_id", F.col("nombre").alias("parque"), "provincia", "operador"), "park_id")
       .withColumn("fabricante", F.when(F.col("modelo").startswith("AE"), "AeroNorte")
                                  .when(F.col("modelo").startswith("BX"), "BoreasX")
                                  .otherwise("CierzoWind")))
dim.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("demo_dim_turbina"))
(spark.table(t("demo_telemetria")).filter("signal = 'power_kw'")
   .groupBy("turbine_id", F.to_date("ts").alias("fecha")).agg((F.sum("value") / 60 / 1000).alias("energia_mwh"))
   .write.mode("overwrite").saveAsTable(t("demo_hechos_energia")))
print("dimensión y hechos creados")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT d.fabricante, d.provincia, round(sum(h.energia_mwh), 1) AS mwh
# MAGIC FROM demo_hechos_energia h JOIN demo_dim_turbina d USING (turbine_id)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY mwh DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tablas, vistas y vistas materializadas

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW demo_v_energia_parque AS
SELECT d.parque, sum(h.energia_mwh) AS mwh
FROM demo_hechos_energia h JOIN demo_dim_turbina d USING (turbine_id)
GROUP BY ALL
""")
display(spark.table("demo_v_energia_parque"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW demo_mv_energia_parque AS SELECT * FROM demo_v_energia_parque

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_mv_energia_parque

# COMMAND ----------

# MAGIC %md
# MAGIC ## NULL: la fuente de la mitad de los bugs

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT turbine_id, ts, LAG(ts) OVER w AS anterior, timestampdiff(MINUTE, LAG(ts) OVER w, ts) AS min_desde_anterior
# MAGIC FROM VALUES ('WTG-1', timestamp'2026-11-02 10:00:00'), ('WTG-1', timestamp'2026-11-02 10:01:00'),
# MAGIC             ('WTG-1', timestamp'2026-11-02 10:07:00'), ('WTG-1', timestamp'2026-11-02 10:08:00') AS t(turbine_id, ts)
# MAGIC WINDOW w AS (PARTITION BY turbine_id ORDER BY ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medias móviles: ROWS o RANGE

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

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM VALUES
# MAGIC   ('WTG-1', timestamp'2026-11-02 10:00:00', 'power_kw', 100.0, timestamp'2026-11-02 10:00:05'),
# MAGIC   ('WTG-1', timestamp'2026-11-02 10:00:00', 'power_kw', 120.0, timestamp'2026-11-02 10:03:00')
# MAGIC   AS t(turbine_id, ts, signal, value, ingested_at)
# MAGIC QUALIFY row_number() OVER (PARTITION BY turbine_id, ts, signal ORDER BY ingested_at DESC) = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v, row_number() OVER (ORDER BY v) AS rn, rank() OVER (ORDER BY v) AS rk, dense_rank() OVER (ORDER BY v) AS drk
# MAGIC FROM VALUES (10), (20), (20), (30) AS t(v)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fechas, franjas y zonas horarias

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

# COMMAND ----------

from pyspark.sql.functions import udf, pandas_udf


def pt(v):
    return 0.0 if v is None or v < 3 or v >= 25 else min(0.5 * 1.225 * math.pi * 75 ** 2 * v ** 3 * 0.45 / 1000, 4200.0)


u = udf(pt, "double")


@pandas_udf("double")
def pu(v: pd.Series) -> pd.Series:
    p = (0.5 * 1.225 * math.pi * 75 ** 2 * v.pow(3) * 0.45 / 1000).clip(upper=4200.0)
    return p.where((v >= 3) & (v < 25), 0.0).fillna(0.0)


nat = (F.when((F.col("v") < 3) | (F.col("v") >= 25), 0.0)
        .otherwise(F.least(F.lit(0.5 * 1.225 * math.pi * 75 ** 2 * 0.45 / 1000) * F.pow("v", 3), F.lit(4200.0))))

df = spark.range(0, 2_000_000).select((F.rand(1) * 20).alias("v"))
for nombre, expr in [("UDF", u("v")), ("Pandas UDF", pu("v")), ("Nativa", nat)]:
    t_ini = time.time()
    df.select(expr.alias("p")).agg(F.sum("p")).collect()
    print(f"{nombre:10s} {time.time() - t_ini:5.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orden de preferencia para extender Spark

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION demo_fn_mwh(kw DOUBLE) RETURNS DOUBLE
COMMENT 'kW medios de un minuto a MWh'
RETURN kw / 60 / 1000
""")
display(spark.sql("SELECT demo_fn_mwh(3000) AS mwh"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE INTO: insertar, actualizar y borrar a la vez

# COMMAND ----------

spark.sql("CREATE OR REPLACE TABLE demo_merge AS SELECT * FROM VALUES ('A', 1), ('B', 2), ('C', 3) AS t(k, v)")
display(spark.table("demo_merge").orderBy("k"))

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO demo_merge t
# MAGIC USING (SELECT * FROM VALUES ('B', 20), ('D', 4) AS s(k, v)) s
# MAGIC ON t.k = s.k
# MAGIC WHEN MATCHED THEN UPDATE SET v = s.v
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC WHEN NOT MATCHED BY SOURCE AND t.k = 'C' THEN DELETE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_merge ORDER BY k   -- A 1, B 20, D 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD tipo 2 con un único MERGE

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

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE demo_scd2_backup DEEP CLONE demo_scd2

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY demo_scd2_backup   -- la versión 0 es la operación CLONE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentar las tablas: el metadato también cuenta

# COMMAND ----------

for s in [
    "COMMENT ON TABLE demo_hechos_energia IS 'Energía diaria por turbina (MWh)'",
    "ALTER TABLE demo_hechos_energia ALTER COLUMN energia_mwh COMMENT 'Suma de la potencia media de cada minuto / 60 / 1000'",
    "ALTER TABLE demo_hechos_energia DROP CONSTRAINT IF EXISTS fk_demo_turbina",
    "ALTER TABLE demo_dim_turbina DROP PRIMARY KEY IF EXISTS",
    "ALTER TABLE demo_dim_turbina ALTER COLUMN turbine_id SET NOT NULL",
    "ALTER TABLE demo_dim_turbina ADD CONSTRAINT pk_demo_turbina PRIMARY KEY (turbine_id)",
    "ALTER TABLE demo_hechos_energia ADD CONSTRAINT fk_demo_turbina FOREIGN KEY (turbine_id) REFERENCES demo_dim_turbina",
    "ALTER TABLE demo_hechos_energia SET TAGS ('dominio' = 'eolica', 'capa' = 'gold')",
]:
    spark.sql(s)
    print("ok:", s[:95])

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED demo_hechos_energia

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza
