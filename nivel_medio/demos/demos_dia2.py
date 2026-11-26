# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2: PySpark, rendimiento y conectividad
# MAGIC
# MAGIC Las celdas que solo tienen comentarios se escriben en clase. El resto ya está hecho: se ejecuta y se mira lo que pasa.

# COMMAND ----------

# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# MAGIC %run ./00_limpieza_demos $dia="2"

# COMMAND ----------

import math, time, datetime as dt


def generar_demo(turbinas, minutos):
    inicio = int(dt.datetime(2026, 11, 2, tzinfo=dt.timezone.utc).timestamp())
    return (turbinas.crossJoin(spark.range(0, minutos).withColumnRenamed("id", "m"))
            .withColumn("ts", F.timestamp_seconds(F.lit(inicio) + F.col("m") * 60))
            .withColumn("wind_speed", F.greatest(F.lit(0.0), F.lit(6.0) + F.lit(3.0) * F.sin(F.col("m") / 229.0) + F.randn(1) * 1.5))
            .withColumn("power_kw", F.least(F.col("potencia_nominal_kw").cast("double"),
                        F.lit(0.5 * 1.225 * math.pi * 0.40 / 1000) * F.pow(F.col("diametro_rotor_m") / 2, 2) * F.pow("wind_speed", 3)))
            .selectExpr("turbine_id", "park_id", "ts", "stack(2, 'wind_speed', wind_speed, 'power_kw', power_kw) as (signal, value)"))


# Telemetría de ejemplo: 60 turbinas, un día, dos señales (172.800 filas)
(generar_demo(df_turbinas().filter("cast(substring(turbine_id, 5) as int) % 4 = 0"), 1440)
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t("demo_telemetria")))

# Copia en JSON para la demo de lectura
RUTA_JSON = f"{RUTA_VOL}/demo_dia2/json"
spark.table(t("demo_telemetria")).write.mode("overwrite").json(RUTA_JSON)

lect = spark.table(t("demo_telemetria"))
maestro = df_turbinas()
print(lect.count(), "filas en lect y", maestro.count(), "turbinas en maestro")

# Scope para la demo de secretos: el del lab 2a si ya existe y, si no, uno de prueba
from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()
_scopes = [s.name for s in _w.secrets.list_scopes()]
if "kv-formacion" in _scopes and "pg-password" in [k.key for k in _w.secrets.list_secrets(scope="kv-formacion")]:
    SCOPE_DEMO = "kv-formacion"
else:
    SCOPE_DEMO = "demo-secretos"
    if SCOPE_DEMO not in _scopes:
        _w.secrets.create_scope(scope=SCOPE_DEMO)
    _w.secrets.put_secret(scope=SCOPE_DEMO, key="pg-password", string_value="valor-de-prueba")
print("Scope para la demo de secretos:", SCOPE_DEMO)


def filas_por_particion(df):
    """Cuántas filas tiene cada partición del DataFrame."""
    return df.groupBy(F.spark_partition_id().alias("particion")).count().orderBy("particion")


def cronometrar(nombre, df):
    """Hace un count de df y dice cuánto ha tardado."""
    t0 = time.time()
    df.count()
    print(f"{nombre}: {time.time() - t0:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Los datos
# MAGIC `lect` tiene una fila por turbina, minuto y señal (`wind_speed` o `power_kw`). `maestro` tiene las 240 turbinas con su parque y su modelo.

# COMMAND ----------

display(lect.limit(10))
display(maestro.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Un clúster es un driver que reparte trabajo
# MAGIC El driver recibe el código, monta el plan y lo parte en tareas. Los executors ejecutan las tareas, cada una sobre una partición de los datos, y devuelven el resultado al driver. En serverless no se ven las máquinas, pero el reparto es el mismo.

# COMMAND ----------

# 20 millones de números agrupados por su último dígito. Al driver solo vuelven 10 filas.
display(spark.range(20_000_000).groupBy((F.col("id") % 10).alias("ultimo_digito")).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué es un DataFrame de Spark
# MAGIC Un DataFrame no lleva los datos dentro. Es un plan con un esquema: qué columnas hay, de qué tipo y cómo se obtienen las filas. Las filas están repartidas en particiones en los executors.

# COMMAND ----------

print(type(lect))
lect.printSchema()
print("particiones:", lect.select(F.spark_partition_id()).distinct().count())

# COMMAND ----------

# En serverless el DataFrame es de Spark Connect y no hay RDD por debajo
try:
    lect.rdd.getNumPartitions()
except Exception as e:
    print("df.rdd no funciona en serverless:", type(e).__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dónde se ejecuta cada línea
# MAGIC El Python de la celda corre en el driver. Las operaciones de Spark corren en los executors. `collect()` trae todas las filas al driver: con pocas filas no pasa nada, con millones el driver se queda sin memoria.

# COMMAND ----------

lect.limit(3).collect()   # pocas filas: bien

# COMMAND ----------

# Trae 2 millones de filas al driver. Mirad lo que tarda la celda.
filas = spark.range(2_000_000).collect()
print(f"{len(filas):,} objetos Row en la memoria del driver")
del filas

# COMMAND ----------

# La suma se hace en los executors y al driver solo llega un número
spark.range(2_000_000).agg(F.sum("id")).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lazy evaluation: nada pasa hasta una acción
# MAGIC - **Transformaciones**: `filter`, `select`, `withColumn`, `groupBy`, `join`... Devuelven otro DataFrame y no ejecutan nada. Spark solo apunta lo que hay que hacer.
# MAGIC - **Acciones**: `count`, `show`, `display`, `collect`, `write`... Piden un resultado. En ese momento Spark junta las transformaciones pendientes en un plan, lo optimiza y lo ejecuta.
# MAGIC
# MAGIC Una cadena larga de transformaciones tarda cero segundos. El tiempo y los errores de datos aparecen en la acción.

# COMMAND ----------

# Solo transformaciones. Mirad lo que tarda la celda.
media_parque = (lect.filter(F.col("signal") == "power_kw")
                    .groupBy("park_id").agg(F.avg("value").alias("media")))

# COMMAND ----------

# La acción: ahora se lee la tabla, se filtra y se agrupa
media_parque.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalyst: el optimizador que reescribe tu código
# MAGIC Antes de ejecutar, Catalyst reescribe el plan. Mueve los filtros lo más cerca posible de la lectura, quita las columnas que no se usan y elige cómo hacer cada join. Tú dices qué quieres y él decide cómo.
# MAGIC
# MAGIC Aquí el filtro está escrito después del `groupBy`. Mirad dónde lo pone Catalyst.

# COMMAND ----------

(lect.groupBy("park_id").agg(F.avg("value"))
     .filter("park_id = 'P03'")).explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leer y escribir con la API de DataFrames
# MAGIC El mismo JSON leído dos veces: dejando que Spark adivine el esquema y dándoselo escrito. Mirad lo que tarda cada celda y qué tipos salen.

# COMMAND ----------

# Lee el JSON de RUTA_JSON sin esquema y enseña el esquema con printSchema.

# COMMAND ----------

# Léelo otra vez con este esquema escrito y enseña el esquema:
# turbine_id STRING, park_id STRING, ts TIMESTAMP, signal STRING, value DOUBLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaciones del día a día
# MAGIC Dos DataFrames con las mismas columnas en distinto orden.

# COMMAND ----------

a = spark.createDataFrame([("WTG-0001", "P01")], "turbine_id string, park_id string")
b = spark.createDataFrame([("P02", "WTG-0002")], "park_id string, turbine_id string")
display(a)
display(b)

# COMMAND ----------

# Junta a y b con union y enséñalo.
# Después júntalos con unionByName y compara.

# COMMAND ----------

# MAGIC %md
# MAGIC Texto que debería ser un número.

# COMMAND ----------

crudo = spark.createDataFrame([("12.5",), ("N/A",), ("-9999",)], "v string")
display(crudo)

# COMMAND ----------

# Añade v_double: v pasado a double con try_cast.

# COMMAND ----------

# MAGIC %md
# MAGIC `transform` encadena funciones que reciben un DataFrame y devuelven otro. Cada paso queda en una función pequeña con nombre, que el viernes se puede testear.

# COMMAND ----------

muestra = spark.createDataFrame([("WTG-0001", 2840.0), ("WTG-0002", -9999.0)], "turbine_id string, value double")
display(muestra)

# COMMAND ----------

# Escribe dos funciones que reciben un DataFrame y devuelven otro:
# quitar_centinelas: value a nulo cuando vale -9999.
# a_mw: añade value_mw = value / 1000.
# Aplícalas a muestra, una detrás de otra, con transform.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tipos de join que vais a usar
# MAGIC Dos lecturas: WTG-0001 está en el maestro y WTG-0999 no.

# COMMAND ----------

huerfana = spark.createDataFrame([("WTG-0001", 1.0), ("WTG-0999", 2.0)], "turbine_id string, value double")
display(huerfana)

# COMMAND ----------

# Con left_semi contra maestro, saca las lecturas cuya turbina está en el maestro.
# Con left_anti, las huérfanas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark o SQL: el mismo motor
# MAGIC La misma consulta escrita en PySpark y en SQL acaba en el mismo plan.

# COMMAND ----------

lect.filter("signal = 'power_kw'").groupBy("park_id").agg(F.avg("value")).explain()
spark.sql(f"SELECT park_id, avg(value) FROM {t('demo_telemetria')} WHERE signal = 'power_kw' GROUP BY park_id").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Los valores que llegan de fuera se pasan a `spark.sql` como parámetros, no pegados en el texto con un f-string.

# COMMAND ----------

# Con spark.sql, cuenta las filas de demo_telemetria de un parque.
# El parque va como parámetro :parque y su valor (P01) en args.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaciones narrow y wide
# MAGIC - **Narrow**: cada partición se procesa sola, sin mirar las demás. `filter`, `select`, `withColumn`. Son baratas.
# MAGIC - **Wide**: necesitan juntar filas de particiones distintas, así que Spark las vuelve a repartir entre las máquinas. Eso es el **shuffle**: escribir a disco, mandar por la red y volver a leer. `groupBy`, `join`, `orderBy`, `distinct`, `repartition`.
# MAGIC
# MAGIC En el plan el shuffle aparece como `Exchange`.

# COMMAND ----------

# Narrow: sin Exchange
lect.filter("signal = 'power_kw'").withColumn("mw", F.col("value") / 1000).explain()

# COMMAND ----------

# Wide: Exchange hashpartitioning(park_id)
lect.groupBy("park_id").count().explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo ejecuta Spark un join
# MAGIC - **Sort-merge**: reparte las dos tablas por la clave (shuffle en los dos lados), ordena cada partición y las cruza. Vale para dos tablas grandes.
# MAGIC - **Broadcast**: copia la tabla pequeña entera a cada máquina y cruza ahí cada partición de la grande. La grande no se mueve. Solo vale si la pequeña cabe en memoria.
# MAGIC
# MAGIC Spark hace broadcast solo cuando ve que una tabla es pequeña. Aquí forzamos cada forma para ver los dos planes. Los tiempos los comparamos más abajo, con una tabla grande.

# COMMAND ----------

lect.join(maestro.hint("merge"), "turbine_id").explain()   # hint("merge") obliga a hacer sort-merge

# COMMAND ----------

lect.join(F.broadcast(maestro), "turbine_id").explain()    # broadcast() obliga a copiar el maestro

# COMMAND ----------

# MAGIC %md
# MAGIC ## Particiones: repartition y coalesce
# MAGIC Cada partición la procesa una tarea, y las tareas van en paralelo.
# MAGIC - `repartition(n)` reparte las filas en n particiones parejas. Hace shuffle.
# MAGIC - `coalesce(n)` junta particiones que ya existen para bajar a n. No hace shuffle, pero solo sirve para bajar.

# COMMAND ----------

display(filas_por_particion(lect))                               # como viene de la tabla

# COMMAND ----------

display(filas_por_particion(lect.repartition(50)))               # 50 particiones parejas

# COMMAND ----------

display(filas_por_particion(lect.repartition(50).coalesce(2)))   # junta las 50 en 2 sin volver a repartir

# COMMAND ----------

# MAGIC %md
# MAGIC ## Caché: cuándo sí y cuándo no
# MAGIC `cache()` guarda un DataFrame en la memoria de los executors para no recalcularlo. Compensa cuando se usa varias veces y cuesta calcularlo. No compensa si se usa una vez o si se lee directamente de una tabla.

# COMMAND ----------

try:
    lect.cache().count()
    print("cache() aceptado en este cómputo")
except Exception as e:
    print("En serverless no hay caché de DataFrames:", str(e).splitlines()[0][:150])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data skew: cuando una tarea hace todo el trabajo
# MAGIC Hay skew cuando una clave tiene muchas más filas que las demás. Al repartir por esa clave, en un `groupBy` o en un `join`, todas sus filas van a la misma partición. Esa tarea tarda mucho más y el resto del clúster se queda esperando. Más particiones no lo arreglan, porque una clave siempre cae entera en una sola.

# COMMAND ----------

# Una tabla grande: 10 millones de filas, el 70 % de P01 y el resto repartido entre P02 y P06
grande = (spark.range(10_000_000)
          .withColumn("park_id", F.when(F.rand(1) < 0.7, "P01")
                      .otherwise(F.concat(F.lit("P0"), (F.rand(2) * 5 + 2).cast("int").cast("string")))))
display(grande.groupBy("park_id").count().orderBy("park_id"))

# COMMAND ----------

# Repartida por park_id en 8 particiones: todo P01 cae en la misma
display(filas_por_particion(grande.repartition(8, "park_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo se arregla el skew, en este orden
# MAGIC 1. **AQE**. La ejecución adaptativa parte las particiones demasiado grandes de un sort-merge join. En serverless está siempre activa.
# MAGIC 1. **Broadcast**, si un lado del join es pequeño. La tabla grande no se reparte, así que el skew no importa.
# MAGIC 1. **Salting**, si los dos lados son grandes. Se añade a la clave un número aleatorio (la sal) para partir la clave dominante en varias particiones. El lado pequeño se replica una vez por cada valor de la sal.

# COMMAND ----------

# Tarifas por parque: 6 filas
tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")
display(tarifas)

# COMMAND ----------

cronometrar("sort-merge", grande.join(tarifas.hint("merge"), "park_id"))
cronometrar("broadcast ", grande.join(F.broadcast(tarifas), "park_id"))

# COMMAND ----------

# Salting: P01 se parte en 8 trozos según la sal
N_SAL = 8
grande_sal = grande.withColumn("sal", (F.rand(4) * N_SAL).cast("int"))
display(filas_por_particion(grande_sal.repartition(8, "park_id", "sal")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spill, Out of Memory y Spark UI
# MAGIC - **Spill**: una tarea no cabe en memoria, Spark escribe parte a disco y sigue. Funciona, pero va mucho más lento. Suele venir de particiones demasiado grandes o de skew.
# MAGIC - **Out of Memory**: ni así cabe y la tarea falla. En el driver pasa con un `collect` grande o con un broadcast de una tabla que no es pequeña.
# MAGIC - En un clúster clásico se ve en el **Spark UI**, pestaña Stages: spill a memoria y a disco, y la duración máxima de las tareas frente a la mediana. En serverless no hay Spark UI. Debajo de cada celda está **See performance**, que abre el Query Profile.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Secretos: ninguna contraseña en el código
# MAGIC Las contraseñas se guardan en un scope de secretos y el código solo pide el valor por su nombre. Así no acaban en el notebook, en Git ni en el historial.

# COMMAND ----------

# Lista los scopes que hay, las claves de SCOPE_DEMO y saca el valor de pg-password con print.
