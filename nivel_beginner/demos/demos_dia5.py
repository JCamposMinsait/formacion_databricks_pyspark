# Databricks notebook source
# MAGIC %md
# MAGIC # Día 5: Del notebook al proyecto, gobierno y consumo
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
faltan = [x for x in ["gold_kpi_parque_dia", "gold_kpi_turbina_dia", "silver_dim_turbina"] if not spark.catalog.tableExists(t(x))]
if faltan:
    print("Faltan", faltan, "y se crean ahora con ponerse_al_dia...")
    dbutils.notebook.run("../labs/00_setup/ponerse_al_dia", 3600, {"empiezo_el_dia": "5"})
print("Tablas Gold listas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Del notebook al módulo
# MAGIC La lógica que se repite sale del notebook a un fichero `.py` con funciones. Así se importa desde cualquier notebook o job, se revisa en Git como cualquier código y, sobre todo, se puede probar con un test automático. `utilidades_demo.py`, en esta carpeta, tiene una función que quita duplicados y convierte el -9999 en nulo.

# COMMAND ----------

import os, sys
sys.path.append(os.getcwd())                       # la carpeta del notebook, donde está utilidades_demo.py
from utilidades_demo import limpiar_lecturas

lecturas = spark.createDataFrame([
    ("WTG-0001", "2026-11-02 10:00:00", "power_kw", 3200.0),
    ("WTG-0001", "2026-11-02 10:00:00", "power_kw", 3200.0),
    ("WTG-0002", "2026-11-02 10:00:00", "power_kw", -9999.0)], "turbine_id string, ts string, signal string, value double")
display(lecturas)
display(limpiar_lecturas(lecturas))

# COMMAND ----------

# MAGIC %md
# MAGIC Un test le da a la función unos datos de entrada pequeños, escritos a mano, y comprueba que devuelve lo que tiene que devolver. Si alguien cambia la función y la rompe, el test falla antes de que llegue a producción.

# COMMAND ----------

# Escribe el DataFrame esperado: WTG-0001 una sola vez con 3200 y WTG-0002 con value nulo (mismas columnas que lecturas).
# Compáralo con limpiar_lecturas(lecturas) usando assertDataFrameEqual (de pyspark.testing).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Permisos y linaje
# MAGIC En Unity Catalog los permisos se dan con `GRANT` y se quitan con `REVOKE`, sobre catálogos, esquemas, tablas o vistas. Para leer una tabla hacen falta tres: `USE CATALOG` en su catálogo, `USE SCHEMA` en su esquema y `SELECT` en la tabla. Los permisos se dan a grupos, no a personas.

# COMMAND ----------

ejecutar([f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.{ESQUEMA} TO `{GRUPO_ANALISTAS}`"])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Da permiso de lectura sobre gold_kpi_parque_dia al grupo `account users` y enseña los permisos de la tabla.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quítale ese permiso y vuelve a enseñar los permisos.

# COMMAND ----------

# MAGIC %md
# MAGIC Unity Catalog apunta solo de qué tablas sale cada tabla (linaje). Se ve en la pestaña **Lineage** de cada tabla en Catalog, y también se puede consultar.

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
# MAGIC Un dashboard de Databricks se monta con consultas SQL sobre las tablas Gold. Cada consulta es un dataset, y cada gráfico se pinta a partir de un dataset. Primero se prueban las consultas aquí y luego se copian al dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT parque, fecha, energia_mwh FROM gold_kpi_parque_dia ORDER BY fecha, parque

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Turbinas a revisar: de gold_kpi_turbina_dia, turbine_id, parque, fecha, rendimiento_medio y vibracion_media
# MAGIC -- de las que tienen rendimiento_medio por debajo de 0,8.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Genie
# MAGIC Genie responde preguntas en lenguaje natural escribiendo SQL sobre las tablas que se le den. Funciona tan bien como lo bien que se le explique el negocio: qué tablas, qué significa cada palabra, alguna consulta de ejemplo y unas preguntas con su respuesta correcta para comprobar que acierta. Esta celda imprime lo que se copia en el espacio de Genie.

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
