# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5b · Gobernanza en Unity Catalog y consumo (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~70 min · Sobre las tablas Gold del día 3.
# MAGIC
# MAGIC > **Cómo usar esta guía**
# MAGIC > - Cada paso tiene un objetivo, la slide del curso donde está explicado y unas pistas.
# MAGIC > - Rellena lo marcado con `___` o `# TODO`. Si una celda no tiene nada que rellenar, léela y ejecútala.
# MAGIC > - Si te atascas: primero la slide, luego la documentación de Databricks y, como último recurso, la solución en `labs/soluciones/`.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

GRUPO = "account users"   # en Free Edition no hay grupos propios: usamos el que incluye a todos los usuarios
spark.sql(f"USE {CATALOGO}.{ESQUEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Permisos
# MAGIC **Objetivo:** dar a un grupo acceso de lectura a tus tablas Gold, y quitárselo.
# MAGIC
# MAGIC **Slide de referencia:** Día 5 · Privilegios en Unity Catalog · GRANT y REVOKE
# MAGIC
# MAGIC **Pistas**
# MAGIC - Para leer una tabla hacen falta tres privilegios: en el catálogo, en el esquema y en la tabla.
# MAGIC - En una empresa, USE CATALOG solo lo da el dueño del catálogo. En Free Edition el catálogo es tuyo, así que puedes dar los tres.

# COMMAND ----------

# TODO: los tres GRANT que necesita el grupo para leer gold_kpi_parque_dia
for sql in [
    f"GRANT ___ ON CATALOG {CATALOGO} TO `{GRUPO}`",
    f"GRANT ___ ON SCHEMA {CATALOGO}.{ESQUEMA} TO `{GRUPO}`",
    f"GRANT ___ ON TABLE gold_kpi_parque_dia TO `{GRUPO}`",
]:
    try:
        spark.sql(sql); print("ok   ", sql)
    except Exception as e:
        print("falla", sql, "→", str(e)[:120])

display(spark.sql("SHOW GRANTS ON TABLE gold_kpi_parque_dia"))

# COMMAND ----------

# TODO: quita el SELECT y comprueba los permisos del grupo sobre tu esquema
spark.sql(f"___")
display(spark.sql(f"SHOW GRANTS `{GRUPO}` ON SCHEMA {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Filtro de filas y máscara de columna
# MAGIC **Objetivo:** que cada usuario vea solo los parques de sus operadores y que las coordenadas exactas solo las vea quien tenga permiso.
# MAGIC
# MAGIC **Slide de referencia:** Día 5 · Filtros de filas y máscaras de columnas
# MAGIC
# MAGIC En una empresa esto se decide con grupos de cuenta y `is_account_group_member`. En Free Edition no hay grupos de cuenta, así que usamos una **tabla de permisos**: quién es cada usuario, qué operadores puede ver y si ve las coordenadas exactas.
# MAGIC
# MAGIC **Pistas**
# MAGIC - `current_user()` devuelve quién está consultando.
# MAGIC - `EXISTS (SELECT 1 FROM tabla WHERE ...)` devuelve true si hay alguna fila que cumpla la condición.
# MAGIC - `SET ROW FILTER función ON (columna)` y `ALTER COLUMN columna SET MASK función`.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_parques AS
SELECT p.*, k.energia_mwh, k.fecha
FROM gold_kpi_parque_dia k
JOIN (SELECT DISTINCT park_id, operador FROM silver_dim_turbina) p USING (park_id)
""")

# Empezáis viendo solo EnerSur y sin coordenadas exactas
spark.sql(f"CREATE OR REPLACE TABLE {t('permisos_operador')} AS SELECT current_user() AS usuario, 'EnerSur' AS operador, false AS ve_coordenadas")

# TODO: el filtro devuelve true si en la tabla de permisos hay una fila para quien consulta y ese operador
spark.sql(f"""
CREATE OR REPLACE FUNCTION {t('rf_operador')}(op STRING)
RETURN EXISTS (SELECT 1 FROM {t('permisos_operador')} p WHERE p.usuario = ___ AND p.operador = op)
""")
# TODO: aplica el filtro a gold_parques sobre la columna operador
spark.sql(f"ALTER TABLE {t('gold_parques')} SET ROW FILTER ___ ON (___)")
display(spark.sql("SELECT operador, count(*) AS filas FROM gold_parques GROUP BY ALL"))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_ubicacion_parques AS SELECT park_id, CAST(lat AS DOUBLE) AS lat, CAST(lon AS DOUBLE) AS lon FROM (VALUES
  ('P01', 37.9512, -6.7533), ('P02', 36.9701, -2.2034), ('P03', 36.0521, -5.6511)) AS t(park_id, lat, lon)
""")
# TODO: quien tiene ve_coordenadas a true ve el valor exacto y el resto, redondeado a un decimal
spark.sql(f"""
CREATE OR REPLACE FUNCTION {t('cm_coordenada')}(v DOUBLE)
RETURN CASE WHEN ___ THEN v ELSE round(v, 1) END
""")
# TODO: aplica la máscara a lat y a lon
spark.sql(f"ALTER TABLE {t('gold_ubicacion_parques')} ALTER COLUMN lat ___")
spark.sql(f"ALTER TABLE {t('gold_ubicacion_parques')} ALTER COLUMN lon ___")
display(spark.table("gold_ubicacion_parques"))

# COMMAND ----------

# TODO: date permiso para el operador 'Viento Atlántico' y para ver coordenadas exactas, y vuelve a consultar
spark.sql(f"INSERT INTO {t('permisos_operador')} VALUES (___)")
display(spark.sql("SELECT operador, count(*) AS filas FROM gold_parques GROUP BY ALL"))
display(spark.table("gold_ubicacion_parques"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Auditoría y coste
# MAGIC > En Free Edition puede que alguna tabla de sistema no esté disponible. Si falla, se ve en la teoría.
# MAGIC **Objetivo:** saber quién ha leído tus tablas y cuánto se ha gastado.
# MAGIC
# MAGIC **Slide de referencia:** Día 5 · Auditoría y linaje con tablas de sistema
# MAGIC
# MAGIC **Pistas**
# MAGIC - Si en tu cuenta no están las tablas de sistema, se ve en la teoría.
# MAGIC - request_params es un map: se accede con request_params['clave'].

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: accesos a tus tablas gold_kpi en system.access.audit
# MAGIC SELECT event_time, user_identity.email, action_name, request_params['full_name_arg'] AS tabla
# MAGIC FROM system.access.audit
# MAGIC WHERE ___
# MAGIC ORDER BY event_time DESC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: DBUs por día y SKU de la última semana en system.billing.usage
# MAGIC SELECT ___
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - 7
# MAGIC GROUP BY ALL ORDER BY 1 DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Databricks SQL o Lakebase
# MAGIC Lakebase se ve en la teoría y no se monta en el laboratorio.
# MAGIC
# MAGIC **Pregunta:** ¿qué usarías para un dashboard que resume millones de lecturas, y qué para una aplicación que consulta la ficha de una turbina miles de veces por minuto?
# MAGIC
# MAGIC **Slide de referencia:** Día 5 · Lakebase: Postgres para aplicaciones
# MAGIC
# MAGIC ## 5 · Genie (UI, ~20 min)
# MAGIC Día 5 · Genie · Cómo conseguir que Genie acierte
# MAGIC 1. Genie → New → añade `gold_kpi_turbina_dia`, `gold_kpi_parque_dia` y `dim_turbina_scd2`.
# MAGIC 2. Escribe tú las **instrucciones**: idioma, qué significa "producción" y "rendimiento", cómo expresar el factor de capacidad y qué es una turbina "a revisar".
# MAGIC 3. Añade una **consulta de ejemplo** en SQL para "turbinas a revisar ayer".
# MAGIC 4. Prueba estas preguntas y revisa el SQL que genera:
# MAGIC    - ¿Qué parque produjo más energía la semana pasada?
# MAGIC    - Evolución diaria del factor de capacidad de Tarifa Levante.
# MAGIC    - ¿Qué turbinas hay que revisar hoy?
# MAGIC 5. Si alguna respuesta es mala, mejora instrucciones o comentarios de columna y repite.
# MAGIC
# MAGIC ## Reto final
# MAGIC Le preguntáis a Genie por la producción de *Costa da Morte* sobre `gold_parques` y no sale nada. ¿Por qué? (pista: sección 2)
