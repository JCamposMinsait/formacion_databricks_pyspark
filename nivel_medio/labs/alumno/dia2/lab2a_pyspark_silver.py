# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2a · Secretos, maestros, PySpark y Silver (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~1 h 30 min
# MAGIC
# MAGIC Al terminar sabrás guardar y leer un secreto sin escribirlo en el código y construir una tabla Silver limpia y cruzada con los maestros.
# MAGIC
# MAGIC > En una empresa, los maestros vendrían de una base de datos por JDBC o con Lakehouse Federation. En Free Edition no se puede llegar a bases de datos de fuera, así que esa parte se ve en la teoría.
# MAGIC
# MAGIC > **Cómo usar esta guía**
# MAGIC > - Cada paso tiene un objetivo, la slide del curso donde está explicado y unas pistas.
# MAGIC > - Rellena lo marcado con `___` o `# TODO`. Si una celda no tiene nada que rellenar, léela y ejecútala.
# MAGIC > - Si te atascas: primero la slide, luego la documentación de Databricks y, como último recurso, la solución.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Secretos
# MAGIC **Objetivo:** guardar una contraseña en un secret scope y leerla sin que aparezca en el código.
# MAGIC
# MAGIC **Slide de referencia:** Día 2 · Secretos: ninguna contraseña en el código
# MAGIC
# MAGIC **Pistas**
# MAGIC - Ejecuta la primera celda, escribe cualquier texto en el cuadro que aparece arriba y ejecuta la segunda.
# MAGIC - `dbutils.secrets.list(scope)` enseña los nombres y `dbutils.secrets.get(scope, clave)` lee un valor.

# COMMAND ----------

dbutils.widgets.text("pg_password", "", "Contraseña de prueba")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
SCOPE = "kv-formacion"
if SCOPE not in [s.name for s in w.secrets.list_scopes()]:
    w.secrets.create_scope(scope=SCOPE)
valor = dbutils.widgets.get("pg_password").strip()
assert valor, "Escribe algo en el cuadro de arriba antes de ejecutar esta celda"
w.secrets.put_secret(scope=SCOPE, key="pg-password", string_value=valor)
dbutils.widgets.removeAll()

# TODO: lista los secretos del scope y lee el de la clave pg-password
display(dbutils.secrets.___(SCOPE))
pwd = dbutils.secrets.___(SCOPE, "pg-password")
print("La contraseña es:", pwd)            # ¿qué se imprime?

# COMMAND ----------

# MAGIC %md
# MAGIC Así quedaría la lectura de los maestros en una empresa, con las credenciales sacadas del scope. En Free Edition no se puede ejecutar, pero es exactamente lo que haríais en vuestro trabajo:
# MAGIC ```python
# MAGIC turbinas = (spark.read.format("jdbc")
# MAGIC     .option("url", "jdbc:postgresql://<host>:5432/activos")
# MAGIC     .option("user", dbutils.secrets.get("kv-formacion", "pg-user"))
# MAGIC     .option("password", dbutils.secrets.get("kv-formacion", "pg-password"))
# MAGIC     .option("query", "SELECT * FROM public.turbinas WHERE estado = 'OPERATIVA'")   # el filtro lo hace la base de datos
# MAGIC     .load())
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Maestros
# MAGIC Los maestros salen del notebook de configuración, como si fueran el extracto que nos pasa el ERP.

# COMMAND ----------

parques  = df_parques()
turbinas = df_turbinas().withColumn("estado", F.lit("OPERATIVA"))
display(turbinas.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Narrow y wide
# MAGIC **Objetivo:** distinguir en el plan qué provoca un shuffle.
# MAGIC
# MAGIC **Slide de referencia:** Día 2 · Transformaciones narrow y wide · Catalyst
# MAGIC
# MAGIC **Pistas**
# MAGIC - En el plan, un shuffle aparece como Exchange.

# COMMAND ----------

bronze = spark.table(t("bronze_telemetria"))

# TODO: narrow → filtra quality = 'GOOD' y convierte la centinela -9999 en nulo
narrow = (bronze.filter(___)
                .withColumn("value", F.when(F.col("value") != -9999, F.col("value"))))
narrow.explain()     # ¿hay algún Exchange?

# TODO: wide → media de value por park_id y signal
wide = narrow.groupBy(___).agg(___)
wide.explain()       # ¿dónde aparece el Exchange?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Silver: limpiar, deduplicar, tipar y enriquecer
# MAGIC **Objetivo:** construir silver_telemetria, la dimensión de turbinas y la cuarentena.
# MAGIC
# MAGIC **Slide de referencia:** Día 2 · Transformaciones del día a día · Tipos de join · Cómo ejecuta Spark un join
# MAGIC
# MAGIC **Pistas**
# MAGIC - Reglas: ts a timestamp · -9999 a nulo · potencias negativas a 0 · fuera duplicados por (turbine_id, ts, signal).
# MAGIC - El maestro es pequeño: usa F.broadcast.
# MAGIC - Las lecturas de turbinas que no están en el maestro van a silver_cuarentena (left_anti o un left join y filtrar nulos).

# COMMAND ----------

limpio = (bronze
    .withColumn("ts", ___)                                                                  # TODO: a timestamp
    .withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
    .withColumn("value", F.when((F.col("signal") == "power_kw") & (F.col("value") < 0), 0.0).otherwise(F.col("value")))
    .dropDuplicates(___))                                                                   # TODO

maestro = (turbinas.select("turbine_id", "park_id", "modelo", "potencia_nominal_kw", "estado")
           .join(parques.select("park_id", F.col("nombre").alias("parque"), "provincia", "operador"), "park_id"))

# TODO: cruza limpio con el maestro (sin park_id, que ya viene en las lecturas) usando broadcast
silver = limpio.join(___, "turbine_id", "left")
silver.explain()      # ¿BroadcastHashJoin?

cuarentena = silver.filter(F.col("modelo").isNull())
silver_ok  = silver.filter(F.col("modelo").isNotNull()).drop("_source_file")

# TODO: guarda silver_telemetria, silver_cuarentena y silver_dim_turbina (overwrite + overwriteSchema)
___
print("silver:", spark.table(t("silver_telemetria")).count(), "· cuarentena:", spark.table(t("silver_cuarentena")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. Reescribe la limpieza de Silver en Spark SQL (CTAS) y comprueba que salen las mismas filas.
# MAGIC 2. Quita el `F.broadcast` y mira el plan. ¿Sigue saliendo BroadcastHashJoin? ¿Por qué?
# MAGIC 3. Cuenta cuántas filas elimina cada regla (duplicados, centinelas, huérfanas).
# MAGIC 4. (Examen) ¿Diferencia entre `union` y `unionByName`? ¿Y entre `dropDuplicates()` y `distinct()`?
