# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 3 · Medallion, SQL avanzado, UDFs y SCD2 (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~2 h · Necesita `silver_telemetria` y `silver_dim_turbina` del lab 2b.
# MAGIC
# MAGIC Al terminar tendrás la telemetría en formato columnar, KPIs con ventanas, la historia del maestro de turbinas en SCD tipo 2 y una Gold documentada que el viernes usarán Power BI y Genie.
# MAGIC
# MAGIC > **Cómo usar esta guía**
# MAGIC > - Cada paso tiene un objetivo, la slide del curso donde está explicado y unas pistas.
# MAGIC > - Rellena lo marcado con `___` o `# TODO`. Si una celda no tiene nada que rellenar, léela y ejecútala.
# MAGIC > - Si te atascas: primero la slide, luego la documentación de Databricks y, como último recurso, la solución en `labs/soluciones/`.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

spark.sql(f"USE {CATALOGO}.{ESQUEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · De EAV a columnar
# MAGIC **Objetivo:** una fila por turbina y minuto, una columna por señal.
# MAGIC
# MAGIC **Slide de referencia:** Día 3 · De EAV a columnar · PIVOT en SQL y en PySpark
# MAGIC
# MAGIC **Pistas**
# MAGIC - Señales: wind_speed, power_kw, rotor_rpm, temp_gearbox_c, vibration_mm_s.
# MAGIC - Solo filas con quality = 'GOOD'.
# MAGIC - Pasa siempre la lista de valores al PIVOT.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: completa el PIVOT
# MAGIC CREATE OR REPLACE TABLE silver_telemetria_wide
# MAGIC CLUSTER BY (turbine_id, ts)
# MAGIC AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT turbine_id, park_id, parque, modelo, potencia_nominal_kw, ts, signal, value
# MAGIC   FROM silver_telemetria
# MAGIC   WHERE ___
# MAGIC )
# MAGIC PIVOT (
# MAGIC   avg(value) FOR signal IN (___)
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM silver_telemetria_wide ORDER BY turbine_id, ts LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Ventanas, COALESCE y NULLIF
# MAGIC **Objetivo:** calcular rampas, huecos, medias móviles y energía sin dividir entre cero.
# MAGIC
# MAGIC **Slide de referencia:** Día 3 · Funciones de ventana · LAG y LEAD en series temporales · Medias móviles: ROWS o RANGE · NULL: la fuente de la mitad de los bugs
# MAGIC
# MAGIC **Pistas**
# MAGIC - Ventana por turbina ordenada por ts.
# MAGIC - Energía de un minuto (kWh) = potencia (kW) / 60.
# MAGIC - NULLIF(x, 0) evita dividir entre cero.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_telemetria_enriquecida AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   ___                                                 AS min_desde_anterior,   -- TODO: minutos desde la lectura anterior (timestampdiff + LAG)
# MAGIC   COALESCE(power_kw, LAG(power_kw) OVER w, 0)         AS power_kw_fill,
# MAGIC   ___                                                 AS rampa_kw,             -- TODO: potencia menos la anterior
# MAGIC   avg(temp_gearbox_c) OVER (PARTITION BY turbine_id ORDER BY ts ROWS BETWEEN ___ AND CURRENT ROW) AS temp_gearbox_mm10,  -- TODO: últimas 10 filas
# MAGIC   ___                                                 AS ratio_carga,          -- TODO: power_kw / potencia_nominal_kw sin dividir entre cero
# MAGIC   COALESCE(power_kw, LAG(power_kw) OVER w, 0) / 60.0  AS energia_kwh
# MAGIC FROM silver_telemetria_wide
# MAGIC WINDOW w AS (PARTITION BY turbine_id ORDER BY ts);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · UDF, Pandas UDF y función nativa
# MAGIC **Objetivo:** calcular la potencia teórica de tres formas y medir cuál es más rápida.
# MAGIC
# MAGIC **Slide de referencia:** Día 3 · UDFs de Python: potentes y lentas · Pandas UDFs: vectorizadas con Arrow · Orden de preferencia para extender Spark
# MAGIC
# MAGIC **Pistas**
# MAGIC - La función Python ya está escrita: conviértela en UDF con F.udf(función, "double").
# MAGIC - La Pandas UDF recibe pandas.Series: usa operaciones vectorizadas (pow, clip, where).

# COMMAND ----------

import math, time
import pandas as pd
from pyspark.sql.functions import udf, pandas_udf

RHO, CP = 1.225, 0.45

def _potencia_teorica(v, rotor_m, nominal_kw):
    if v is None or v < 3 or v >= 25:
        return 0.0
    area = math.pi * (rotor_m / 2) ** 2
    return min(0.5 * RHO * area * v ** 3 * CP / 1000.0, float(nominal_kw))

# TODO 3.a · UDF de Python fila a fila
potencia_udf = ___

# TODO 3.b · Pandas UDF vectorizada
@pandas_udf("double")
def potencia_pudf(v: pd.Series, rotor_m: pd.Series, nominal_kw: pd.Series) -> pd.Series:
    area = math.pi * (rotor_m / 2) ** 2
    p = ___                                         # ½·ρ·A·v³·Cp / 1000
    p = p.clip(upper=nominal_kw.astype(float))
    return p.where(___, 0.0).fillna(0.0)            # 0 fuera de [3, 25) m/s

rotor = df_turbinas().select("turbine_id", "diametro_rotor_m")
df = spark.table(t("silver_telemetria_enriquecida")).join(rotor, "turbine_id")

for nombre, f in [("UDF Python", potencia_udf), ("Pandas UDF", potencia_pudf)]:
    t0 = time.time()
    df.select(f("wind_speed", "diametro_rotor_m", "potencia_nominal_kw").alias("p")).agg(F.sum("p")).collect()
    print(f"{nombre:12s} → {time.time() - t0:5.1f} s")

# TODO 3.c · La misma fórmula solo con funciones de Spark (F.when, F.least, F.pow) y mide el tiempo

# COMMAND ----------

# Guardamos la Silver final con potencia teórica y rendimiento (real / teórica)
(df.withColumn("potencia_teorica_kw", potencia_pudf("wind_speed", "diametro_rotor_m", "potencia_nominal_kw"))
   .withColumn("rendimiento", F.col("power_kw") / F.when(F.col("potencia_teorica_kw") != 0, F.col("potencia_teorica_kw")))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("silver_telemetria_final")))

# TODO: registra la fórmula como SQL UDF en Unity Catalog con el nombre fn_potencia_teorica y pruébala con (10, 150, 4200)
spark.sql(f"""
CREATE OR REPLACE FUNCTION {t('fn_potencia_teorica')}(v DOUBLE, rotor_m DOUBLE, nominal_kw DOUBLE)
RETURNS DOUBLE
RETURN ___
""")
display(spark.sql(f"SELECT {t('fn_potencia_teorica')}(10, 150, 4200) AS kw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · MERGE y SCD tipo 2
# MAGIC **Objetivo:** guardar la historia del estado de cada turbina.
# MAGIC
# MAGIC **Slide de referencia:** Día 3 · SCD: cómo guardar la historia de un maestro · SCD tipo 2 con un único MERGE
# MAGIC
# MAGIC **Pistas**
# MAGIC - Las filas que cambian entran dos veces en el USING: una con merge_key = turbine_id (cierra la vieja) y otra con merge_key = NULL (inserta la nueva).
# MAGIC - Relanza el MERGE al terminar: no debería cambiar nada.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dim_turbina_scd2 (
# MAGIC   turbine_id STRING, park_id STRING, modelo STRING, potencia_nominal_kw INT, estado STRING,
# MAGIC   valido_desde TIMESTAMP, valido_hasta TIMESTAMP, es_actual BOOLEAN);
# MAGIC
# MAGIC INSERT INTO dim_turbina_scd2
# MAGIC SELECT turbine_id, park_id, modelo, potencia_nominal_kw, estado, timestamp'2000-01-01', NULL, true
# MAGIC FROM silver_dim_turbina
# MAGIC WHERE NOT EXISTS (SELECT 1 FROM dim_turbina_scd2);

# COMMAND ----------

# Simulamos el nuevo extracto del origen: 10 turbinas pasan a MANTENIMIENTO
# (si habéis ejecutado el UPDATE del lab2a en Postgres, leed de JDBC en vez de esto)
spark.table(t("dim_turbina_scd2")).filter("es_actual").drop("valido_desde", "valido_hasta", "es_actual") \
    .withColumn("estado", F.when(F.col("turbine_id").isin([f"WTG-{i:04d}" for i in range(1, 11)]), "MANTENIMIENTO").otherwise(F.col("estado"))) \
    .createOrReplaceTempView("extracto_turbinas")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: completa el MERGE SCD2
# MAGIC MERGE INTO dim_turbina_scd2 AS d
# MAGIC USING (
# MAGIC   SELECT e.turbine_id AS merge_key, e.* FROM extracto_turbinas e
# MAGIC   UNION ALL
# MAGIC   SELECT ___ AS merge_key, e.* FROM extracto_turbinas e
# MAGIC   JOIN dim_turbina_scd2 d ON e.turbine_id = d.turbine_id AND d.es_actual AND ___
# MAGIC ) AS s
# MAGIC ON d.turbine_id = s.merge_key AND d.es_actual
# MAGIC WHEN MATCHED AND d.estado <> s.estado THEN
# MAGIC   UPDATE SET ___
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (turbine_id, park_id, modelo, potencia_nominal_kw, estado, valido_desde, valido_hasta, es_actual)
# MAGIC   VALUES (___);
# MAGIC
# MAGIC SELECT * FROM dim_turbina_scd2 WHERE turbine_id IN ('WTG-0001','WTG-0002','WTG-0050') ORDER BY turbine_id, valido_desde;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Gold: KPIs de negocio
# MAGIC **Objetivo:** una tabla por turbina y día y otra por parque y día, documentadas.
# MAGIC
# MAGIC **Slide de referencia:** Día 3 · Cómo modelar la capa Gold · La Gold del lab: KPIs eólicos
# MAGIC
# MAGIC **Pistas**
# MAGIC - energia_mwh = sum(energia_kwh) / 1000.
# MAGIC - factor_capacidad = sum(energia_kwh) / (potencia_nominal_kw × 24).
# MAGIC - disponibilidad = minutos con viento entre 3 y 25 y potencia > 0 / minutos con viento entre 3 y 25 (count_if).
# MAGIC - Documenta las columnas con ALTER TABLE ... ALTER COLUMN ... COMMENT.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: completa los KPIs
# MAGIC CREATE OR REPLACE TABLE gold_kpi_turbina_dia
# MAGIC COMMENT 'KPIs diarios por aerogenerador'
# MAGIC AS
# MAGIC SELECT
# MAGIC   date(ts) AS fecha, turbine_id, park_id, parque, modelo, potencia_nominal_kw,
# MAGIC   ___ AS energia_mwh,
# MAGIC   ___ AS factor_capacidad,
# MAGIC   ___ AS disponibilidad,
# MAGIC   round(avg(rendimiento), 3)      AS rendimiento_medio,
# MAGIC   round(max(temp_gearbox_c), 1)   AS temp_gearbox_max,
# MAGIC   round(avg(vibration_mm_s), 3)   AS vibracion_media
# MAGIC FROM silver_telemetria_final
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: agregado por parque y día: turbinas, energía total, factor de capacidad medio y disponibilidad media
# MAGIC CREATE OR REPLACE TABLE gold_kpi_parque_dia AS
# MAGIC SELECT ___
# MAGIC FROM gold_kpi_turbina_dia
# MAGIC GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: documenta al menos factor_capacidad, disponibilidad y rendimiento_medio (Genie lo usará el día 5)
# MAGIC ALTER TABLE gold_kpi_turbina_dia ALTER COLUMN factor_capacidad COMMENT '___';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. Lista de mantenimiento: turbinas con peor rendimiento medio y vibración por encima del percentil 90. (Hay 3 turbinas degradadas escondidas: ¿las encuentras?)
# MAGIC 2. Con `LAG`, cuenta los huecos de comunicación de más de 5 minutos por parque.
# MAGIC 3. Pandas UDF de agregación (Series → float) que devuelva la energía teórica del día.
# MAGIC 4. Consulta point-in-time: energía de un día cruzada con el estado que tenía cada turbina ese día en `dim_turbina_scd2`.
# MAGIC 5. (Examen) Vista, vista temporal, tabla y vista materializada: ¿cuál para un dashboard que se refresca cada hora?
