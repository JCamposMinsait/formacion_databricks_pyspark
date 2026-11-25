# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2a · Secretos, maestros, PySpark y Silver
# MAGIC
# MAGIC Duración: 1 h 30 min aproximadamente.
# MAGIC
# MAGIC Vamos a guardar y leer un secreto sin escribirlo en el código y a construir una tabla Silver limpia y cruzada con los maestros.
# MAGIC
# MAGIC > En una empresa, los maestros vendrían de una base de datos por JDBC o con Lakehouse Federation. En Free Edition no se puede llegar a bases de datos de fuera, así que esa parte se ve en la teoría.
# MAGIC
# MAGIC En las celdas hay que completar lo que está marcado con `___` o `# TODO`. Los retos del final son opcionales.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Secretos
# MAGIC Guardamos una contraseña en un secret scope y la leemos sin que aparezca en el código.
# MAGIC - Ejecuta la primera celda, escribe cualquier texto en el cuadro que aparece arriba y ejecuta la segunda.
# MAGIC - `dbutils.secrets.list(scope)` enseña los nombres y `dbutils.secrets.get(scope, clave)` lee un valor.
# MAGIC
# MAGIC Lo tienes en la slide Día 2 · Secretos: ninguna contraseña en el código.

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
# MAGIC Así se leerían los maestros en una empresa, con las credenciales sacadas del scope. En Free Edition no se puede ejecutar.
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
# MAGIC Vamos a ver qué operaciones provocan un shuffle. En el plan, el shuffle aparece como Exchange.
# MAGIC
# MAGIC Lo tienes en las slides Día 2 · Transformaciones narrow y wide · Catalyst.

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
# MAGIC Construimos `silver_telemetria`, la dimensión de turbinas y la cuarentena.
# MAGIC - Las reglas son: `ts` a timestamp, `-9999` a nulo, potencias negativas a 0 y fuera los duplicados por (turbine_id, ts, signal).
# MAGIC - El maestro es pequeño, así que usa `F.broadcast`.
# MAGIC - Las lecturas de turbinas que no están en el maestro van a `silver_cuarentena` (con un `left_anti`, o con un left join y filtrando los nulos).
# MAGIC
# MAGIC Lo tienes en las slides Día 2 · Transformaciones del día a día · Tipos de join · Cómo ejecuta Spark un join.

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
