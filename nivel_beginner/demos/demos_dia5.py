# Databricks notebook source
# MAGIC %md
# MAGIC # Día 5: Del notebook al proyecto, gobierno y consumo

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
faltan = [x for x in ["gold_kpi_parque_dia", "gold_kpi_turbina_dia", "silver_dim_turbina"] if not spark.catalog.tableExists(t(x))]
if faltan:
    print("Faltan", faltan, "y se crean ahora con ponerse_al_dia...")
    dbutils.notebook.run("../labs/00_setup/ponerse_al_dia", 3600, {"empiezo_el_dia": "5"})
print("Tablas Gold listas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Del notebook al módulo

# COMMAND ----------

import os, sys
sys.path.append(os.getcwd())                       # la carpeta del notebook, donde está utilidades_demo.py
from utilidades_demo import limpiar_lecturas

lecturas = spark.createDataFrame([
    ("WTG-0001", "2026-11-02 10:00:00", "power_kw", 3200.0),
    ("WTG-0001", "2026-11-02 10:00:00", "power_kw", 3200.0),
    ("WTG-0002", "2026-11-02 10:00:00", "power_kw", -9999.0)], "turbine_id string, ts string, signal string, value double")
display(limpiar_lecturas(lecturas))

# COMMAND ----------

from pyspark.testing import assertDataFrameEqual

esperado = spark.createDataFrame([
    ("WTG-0001", "2026-11-02 10:00:00", "power_kw", 3200.0),
    ("WTG-0002", "2026-11-02 10:00:00", "power_kw", None)], "turbine_id string, ts string, signal string, value double")
assertDataFrameEqual(limpiar_lecturas(lecturas), esperado)
print("Test superado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Permisos y linaje

# COMMAND ----------

ejecutar([
    f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.{ESQUEMA} TO `{GRUPO_ANALISTAS}`",
    f"GRANT SELECT ON TABLE {t('gold_kpi_parque_dia')} TO `{GRUPO_ANALISTAS}`",
])
display(spark.sql(f"SHOW GRANTS ON TABLE {t('gold_kpi_parque_dia')}"))

# COMMAND ----------

ejecutar([f"REVOKE SELECT ON TABLE {t('gold_kpi_parque_dia')} FROM `{GRUPO_ANALISTAS}`"])
display(spark.sql(f"SHOW GRANTS ON TABLE {t('gold_kpi_parque_dia')}"))

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT source_table_full_name, target_table_full_name, max(event_time) AS ultima_vez
        FROM system.access.table_lineage
        WHERE target_table_full_name LIKE '{CATALOGO}.{ESQUEMA}.gold%'
        GROUP BY ALL ORDER BY target_table_full_name"""))
except Exception as e:
    print("Las tablas de sistema no están disponibles en esta cuenta:", str(e).splitlines()[0][:120])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Un dashboard

# COMMAND ----------

CONSULTAS = {
    "Energía por parque y día": f"SELECT parque, fecha, energia_mwh FROM {t('gold_kpi_parque_dia')}",
    "Turbinas a revisar": f"SELECT turbine_id, parque, fecha, rendimiento_medio, vibracion_media FROM {t('gold_kpi_turbina_dia')} WHERE rendimiento_medio < 0.8",
}
for nombre, consulta in CONSULTAS.items():
    print(f"{nombre}\n{consulta}\n")
display(spark.sql(CONSULTAS["Energía por parque y día"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Genie

# COMMAND ----------

TABLAS_GENIE = [t(x) for x in ["gold_kpi_turbina_dia", "gold_kpi_parque_dia", "silver_dim_turbina"]]

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
