# Databricks notebook source
# MAGIC %md
# MAGIC # Día 5: DataOps, gobernanza y consumo
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="5"

# COMMAND ----------

GRUPO_ANALISTAS = "account users"   # en Free Edition no hay grupos propios: el que incluye a todos los usuarios


def ejecutar(sentencias):
    for s in sentencias:
        try:
            spark.sql(s)
            print("ok   ", s)
        except Exception as e:
            print("falla", s, "\n      ", str(e).splitlines()[0][:150])


# Las demos de hoy usan las tablas Gold del lab 3. Si no están, se crean con ponerse_al_dia (tarda unos minutos)
faltan = [x for x in ["gold_kpi_parque_dia", "gold_kpi_turbina_dia", "dim_turbina_scd2"] if not spark.catalog.tableExists(t(x))]
if faltan:
    print("Faltan", faltan, "y se crean ahora con ponerse_al_dia...")
    dbutils.notebook.run("../labs/00_setup/ponerse_al_dia", 3600, {"empiezo_el_dia": "5"})
print("Tablas Gold listas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tests con pytest y Spark local
# MAGIC Las transformaciones se escriben como funciones que reciben y devuelven un DataFrame. Un test les da unas pocas filas escritas a mano y comprueba el resultado. En el proyecto, el test vive en `tests/`, lo lanza pytest con un Spark local y se ejecuta en cada cambio en GitHub Actions, sin abrir Databricks.

# COMMAND ----------

def limpiar_lecturas(df):
    return (df.withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
              .dropDuplicates(["turbine_id", "ts", "signal"]))


esquema = "turbine_id string, ts int, signal string, value double"
entrada = spark.createDataFrame([("WTG-1", 1, "power_kw", -9999.0), ("WTG-1", 1, "power_kw", -9999.0), ("WTG-2", 1, "power_kw", 50.0)], esquema)
display(entrada)

# COMMAND ----------

# Escribe el DataFrame esperado de limpiar_lecturas(entrada), con el mismo esquema,
# y compáralo con assertDataFrameEqual (de pyspark.testing).

# COMMAND ----------

# MAGIC %md
# MAGIC ## GRANT y REVOKE
# MAGIC Para leer una tabla hacen falta `USE CATALOG`, `USE SCHEMA` y `SELECT`. Los permisos se dan a grupos y se heredan: un `SELECT` sobre el esquema vale para todas sus tablas, también las que se creen después.

# COMMAND ----------

ejecutar([f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.{ESQUEMA} TO `{GRUPO_ANALISTAS}`"])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Da SELECT sobre gold_kpi_parque_dia al grupo `account users` y enseña los permisos de la tabla.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quítalo y vuelve a enseñar los permisos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtros de filas y máscaras de columnas
# MAGIC Con un filtro de filas, cada usuario ve solo las filas que le tocan. Con una máscara, ve una columna transformada (redondeada, oculta...). Las dos son funciones SQL que se enganchan a la tabla, y deciden según quién consulta. Aquí, una tabla de permisos dice qué operadores ve cada usuario y si ve las coordenadas exactas.

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS demo_parques")
spark.sql("""
CREATE TABLE demo_parques AS
SELECT park_id, nombre, operador, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon
FROM VALUES
  ('P01', 'Sierra Norte',   'EnerSur',          37.9512, -6.7533),
  ('P03', 'Tarifa Levante', 'Viento Atlántico', 36.0521, -5.6511),
  ('P05', 'Muela de Borja', 'Cierzo Energía',   41.8012, -1.5533)
  AS t(park_id, nombre, operador, lat, lon)
""")
spark.sql(f"CREATE OR REPLACE TABLE {t('demo_permisos')} AS SELECT current_user() AS usuario, 'EnerSur' AS operador, false AS ve_coordenadas")
display(spark.table(t("demo_parques")))
display(spark.table(t("demo_permisos")))

# COMMAND ----------

ejecutar([
    f"CREATE OR REPLACE FUNCTION {t('demo_rf')}(op STRING) RETURN EXISTS (SELECT 1 FROM {t('demo_permisos')} p WHERE p.usuario = current_user() AND p.operador = op)",
    f"CREATE OR REPLACE FUNCTION {t('demo_cm')}(v DOUBLE) RETURN CASE WHEN EXISTS (SELECT 1 FROM {t('demo_permisos')} p WHERE p.usuario = current_user() AND p.ve_coordenadas) THEN v ELSE round(v, 1) END",
    f"ALTER TABLE {t('demo_parques')} SET ROW FILTER {t('demo_rf')} ON (operador)",
    f"ALTER TABLE {t('demo_parques')} ALTER COLUMN lat SET MASK {t('demo_cm')}",
])
display(spark.table(t("demo_parques")))

# COMMAND ----------

# Te das permiso para otro operador y para ver las coordenadas exactas
spark.sql(f"INSERT INTO {t('demo_permisos')} VALUES (current_user(), 'Viento Atlántico', true)")
display(spark.table(t("demo_parques")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditoría y linaje con tablas de sistema
# MAGIC Unity Catalog apunta quién accede a qué (`system.access.audit`) y de qué tablas sale cada tabla (`system.access.table_lineage`). Se consultan con SQL como cualquier tabla. Hace falta permiso sobre el catálogo `system`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_time, user_identity.email, action_name, request_params['full_name_arg'] AS tabla
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name = 'getTable' AND request_params['full_name_arg'] LIKE '%gold_kpi%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table_full_name, target_table_full_name, max(event_time) AS ultima_vez
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE '%gold_kpi%'
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo se paga Databricks
# MAGIC Se paga en DBUs, unidades de cómputo, más el coste de las máquinas si el cómputo es clásico. Cada tipo de cómputo (serverless, jobs, SQL warehouse...) tiene su precio por DBU. El consumo detallado está en `system.billing.usage`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT usage_date, sku_name, round(sum(usage_quantity), 2) AS dbus
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - 7
# MAGIC GROUP BY ALL
# MAGIC ORDER BY usage_date DESC, dbus DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Un dashboard de Databricks paso a paso
# MAGIC Cada gráfico de un dashboard sale de un dataset, que es una consulta SQL. Las consultas se prueban aquí y se copian al dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dataset 1: de gold_kpi_parque_dia, fecha, parque, energia_mwh y el factor de capacidad medio en porcentaje.

# COMMAND ----------

# Dataset 2: turbinas a revisar por bajo rendimiento, vibración alta para su día o multiplicadora caliente
DATASET_2 = f"""
WITH p AS (SELECT fecha, percentile(vibracion_media, 0.9) AS p90 FROM {t('gold_kpi_turbina_dia')} GROUP BY fecha)
SELECT k.fecha, k.turbine_id, k.parque, k.rendimiento_medio, k.vibracion_media, k.temp_gearbox_max
FROM {t('gold_kpi_turbina_dia')} k JOIN p USING (fecha)
WHERE k.rendimiento_medio < 0.8 OR k.vibracion_media > p.p90 OR k.temp_gearbox_max > 78"""
print(DATASET_2)
display(spark.sql(DATASET_2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurar un Genie space paso a paso
# MAGIC Genie escribe SQL a partir de preguntas en lenguaje natural. Acierta en la medida en que se le explique el negocio: qué tablas, qué significa cada palabra, consultas de ejemplo y preguntas con su respuesta correcta (benchmarks) para medir si acierta. Esta celda imprime lo que se copia en el espacio.

# COMMAND ----------

TABLAS_GENIE = [t(x) for x in ["gold_kpi_turbina_dia", "gold_kpi_parque_dia", "dim_turbina_scd2"]]

INSTRUCCIONES_GENIE = """- Responde siempre en español.
- Producción o generación significa energia_mwh.
- El factor de capacidad se muestra en porcentaje (multiplicado por 100) con un decimal.
- Rendimiento es rendimiento_medio. Por debajo de 0,8 se considera bajo.
- Una turbina a revisar es la que tiene rendimiento_medio < 0,8 y vibracion_media por encima del percentil 90 del mismo día.
- Nombra los parques por su nombre (columna parque), no por el código.
- Si no se indica fecha, usa el último día disponible."""

CONSULTA_EJEMPLO = f"""-- ¿Qué turbinas hay que revisar hoy?
WITH d AS (SELECT * FROM {t('gold_kpi_turbina_dia')} WHERE fecha = (SELECT max(fecha) FROM {t('gold_kpi_turbina_dia')})),
     p AS (SELECT percentile(vibracion_media, 0.9) AS p90 FROM d)
SELECT d.turbine_id, d.parque, d.rendimiento_medio, d.vibracion_media
FROM d CROSS JOIN p
WHERE d.rendimiento_medio < 0.8 AND d.vibracion_media > p.p90"""

BENCHMARKS = [
    ("¿Qué parque produjo más energía el último día?",
     f"SELECT parque FROM {t('gold_kpi_parque_dia')} WHERE fecha = (SELECT max(fecha) FROM {t('gold_kpi_parque_dia')}) ORDER BY energia_mwh DESC LIMIT 1"),
    ("¿Cuántas turbinas tiene cada parque?",
     f"SELECT parque, count(DISTINCT turbine_id) FROM {t('gold_kpi_turbina_dia')} GROUP BY parque"),
    ("¿Qué turbinas tienen la multiplicadora más caliente?",
     f"SELECT turbine_id, max(temp_gearbox_max) AS t FROM {t('gold_kpi_turbina_dia')} GROUP BY turbine_id ORDER BY t DESC LIMIT 5"),
]

print("TABLAS\n" + "\n".join(TABLAS_GENIE))
print("\nINSTRUCCIONES\n" + INSTRUCCIONES_GENIE)
print("\nCONSULTA DE EJEMPLO\n" + CONSULTA_EJEMPLO)
print("\nBENCHMARKS")
for pregunta, sql in BENCHMARKS:
    print(f"- {pregunta}\n  {sql}")
