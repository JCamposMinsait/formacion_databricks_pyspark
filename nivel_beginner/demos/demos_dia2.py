# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2: PySpark y rendimiento básico
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
maestro = df_turbinas().join(df_parques().select("park_id", F.col("nombre").alias("parque")), "park_id")
print(lect.count(), "filas en lect y", maestro.count(), "turbinas en maestro")


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
# MAGIC `lect` tiene una fila por turbina, minuto y señal (`wind_speed` o `power_kw`). `maestro` tiene una fila por turbina con su parque y su modelo.

# COMMAND ----------

display(lect.limit(10))
display(maestro.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer
# MAGIC El mismo JSON leído dos veces: dejando que Spark adivine el esquema y dándoselo escrito. Mirad lo que tarda cada celda y qué tipos salen.

# COMMAND ----------

# Lee el JSON de RUTA_JSON sin esquema y enseña el esquema con printSchema.

# COMMAND ----------

# Léelo otra vez con este esquema escrito y enseña el esquema:
# turbine_id STRING, park_id STRING, ts TIMESTAMP, signal STRING, value DOUBLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformar

# COMMAND ----------

# A partir de lect, paso a paso y ejecutando después de cada línea:
# 1. Quédate con las lecturas de potencia (signal igual a power_kw).
# 2. Elige turbine_id, ts y value.
# 3. Añade potencia_mw: value entre 1000, redondeado a 3 decimales.
# 4. Añade estado: ALTA por encima de 3000 kW, BAJA por encima de 0 y PARADA en el resto.
# 5. Enseña 10 filas.

# COMMAND ----------

# MAGIC %md
# MAGIC Datos sucios: cuando el sensor falla manda texto, nulos o -9999.

# COMMAND ----------

sucio = spark.createDataFrame([("WTG-0001", "12.5"), ("WTG-0002", "N/A"), ("WTG-0003", None), ("WTG-0004", "-9999")],
                              "turbine_id string, valor string")
display(sucio)

# COMMAND ----------

# Sobre sucio, añade tres columnas, una cada vez:
# como_numero: valor pasado a double con try_cast (N/A queda en nulo en vez de romper).
# sin_centinela: como_numero, pero nulo cuando vale -9999.
# con_cero: sin_centinela con los nulos cambiados por 0.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agrupar y unir

# COMMAND ----------

# Guarda en potencia las lecturas de power_kw de lect.
# Saca por parque la potencia media (redondeada a 1 decimal) y el número de lecturas, ordenado por parque.

# COMMAND ----------

# Une potencia con maestro por turbine_id (del maestro solo turbine_id, parque y modelo)
# y saca la potencia media por parque y modelo, ordenado por los dos.

# COMMAND ----------

# MAGIC %md
# MAGIC Una lectura de una turbina que no está en el maestro.

# COMMAND ----------

extra = spark.createDataFrame([("WTG-0999", "power_kw", 1500.0)], "turbine_id string, signal string, value double")
display(extra)

# COMMAND ----------

# Con un join left_anti contra maestro, saca las lecturas de extra cuya turbina no está en el maestro.

# COMMAND ----------

# MAGIC %md
# MAGIC Lo mismo en SQL. Una vista temporal deja consultar un DataFrame desde SQL mientras dura la sesión.

# COMMAND ----------

potencia.createOrReplaceTempView("potencia_v")
maestro.createOrReplaceTempView("maestro_v")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Potencia media por parque, con el nombre del parque: une potencia_v y maestro_v por turbine_id.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Lazy y planes
# MAGIC Hay dos tipos de operaciones.
# MAGIC
# MAGIC - **Transformaciones**: `filter`, `select`, `withColumn`, `groupBy`, `join`, `orderBy`... Devuelven otro DataFrame y no ejecutan nada. Spark solo apunta lo que hay que hacer.
# MAGIC - **Acciones**: `count`, `show`, `display`, `collect`, `write`... Piden un resultado. En ese momento Spark junta todas las transformaciones pendientes en un plan, lo optimiza y lo ejecuta.
# MAGIC
# MAGIC Por eso una cadena larga de transformaciones tarda cero segundos y todo el tiempo se va en la acción. Y por eso un error en los datos, como el `N/A` de antes con un cast normal, no salta hasta la acción.

# COMMAND ----------

# Tres transformaciones encadenadas. Mirad lo que tarda la celda.
plan = (lect.filter(F.col("signal") == "wind_speed")
            .withColumn("doble", F.col("value") * 2)
            .groupBy("turbine_id").agg(F.max("doble")))

# COMMAND ----------

# Una acción: ahora sí se lee la tabla y se hace todo lo de arriba
plan.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Las transformaciones también se dividen según si mueven datos o no.
# MAGIC
# MAGIC - **Narrow**: cada partición se procesa sola, sin mirar las demás. `filter`, `select`, `withColumn`. Son baratas.
# MAGIC - **Wide**: necesitan juntar filas que están en particiones distintas, así que Spark las vuelve a repartir entre las máquinas. Eso es el **shuffle**: escribir a disco, mandar por la red y volver a leer. `groupBy`, `join`, `orderBy`, `distinct`, `repartition`.
# MAGIC
# MAGIC `explain()` enseña el plan que va a ejecutar Spark. El shuffle aparece como `Exchange` y es lo primero que se busca cuando algo va lento.

# COMMAND ----------

# Narrow: en el plan no hay Exchange
lect.filter(F.col("signal") == "wind_speed").withColumn("doble", F.col("value") * 2).explain()

# COMMAND ----------

# Wide: groupBy tiene que juntar las filas de cada turbina y aparece un Exchange
lect.groupBy("turbine_id").count().explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Particiones
# MAGIC Un DataFrame está partido en trozos llamados particiones. Cada partición la procesa una tarea y las tareas van en paralelo. Con pocas particiones se desaprovechan máquinas. Si una es mucho más grande que el resto, todas acaban esperando por ella.
# MAGIC
# MAGIC - `repartition(n)` reparte las filas en n particiones parejas. Hace shuffle.
# MAGIC - `coalesce(n)` junta particiones que ya existen para bajar a n. No hace shuffle y es más barato, pero solo sirve para bajar.

# COMMAND ----------

display(filas_por_particion(lect))                              # como viene de la tabla

# COMMAND ----------

display(filas_por_particion(lect.repartition(8)))               # 8 particiones parejas

# COMMAND ----------

display(filas_por_particion(lect.repartition(8).coalesce(2)))   # junta las 8 en 2 sin volver a repartir

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Skew y broadcast
# MAGIC Hay skew cuando una clave tiene muchas más filas que las demás. Al repartir por esa clave, en un `groupBy` o en un `join`, todas sus filas van a la misma partición. Esa tarea tarda mucho más y el resto del clúster se queda esperando. Poner más particiones no lo arregla, porque una clave siempre cae entera en una sola.

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
# MAGIC Spark tiene dos formas principales de hacer un join.
# MAGIC
# MAGIC - **Sort-merge**: reparte las dos tablas por la clave (shuffle en los dos lados), ordena cada partición y las cruza. Sirve para dos tablas grandes, pero sufre con el skew porque todo P01 va a la misma tarea.
# MAGIC - **Broadcast**: copia la tabla pequeña entera a cada máquina y cruza ahí cada partición de la grande. La grande no se mueve, así que no hay shuffle ni skew. Solo vale si la pequeña cabe en memoria.
# MAGIC
# MAGIC Spark hace broadcast solo cuando ve que una tabla es pequeña. Aquí forzamos cada forma para comparar.

# COMMAND ----------

# Tarifas por parque: 6 filas
tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")
display(tarifas)

# COMMAND ----------

cronometrar("sort-merge", grande.join(tarifas.hint("merge"), "park_id"))   # hint("merge") obliga a hacer sort-merge
cronometrar("broadcast ", grande.join(F.broadcast(tarifas), "park_id"))     # broadcast() obliga a copiar tarifas

# COMMAND ----------

# El porqué está en el plan: Exchange en los dos lados contra BroadcastExchange solo en tarifas
grande.join(tarifas.hint("merge"), "park_id").explain()
grande.join(F.broadcast(tarifas), "park_id").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. UDF o función de Spark
# MAGIC Una UDF es una función de Python normal que Spark aplica fila a fila. Para eso manda cada fila a un proceso de Python y trae el resultado de vuelta. Las funciones de `F` se ejecutan dentro del motor, sin salir de él, y el optimizador las entiende. Calculamos la misma potencia teórica de las dos formas sobre 2 millones de velocidades de viento.

# COMMAND ----------

from pyspark.sql.functions import udf

K = 0.5 * 1.225 * math.pi * 75 ** 2 * 0.45 / 1000   # kW por (m/s)^3 con un rotor de 150 m


def potencia_teorica(v):
    return 0.0 if v < 3 else min(K * v ** 3, 4200.0)


con_udf = udf(potencia_teorica, "double")
nativa = F.when(F.col("v") < 3, 0.0).otherwise(F.least(F.lit(K) * F.pow("v", 3), F.lit(4200.0)))

datos = spark.range(2_000_000).select((F.rand(1) * 20).alias("v"))
cronometrar("UDF               ", datos.select(con_udf("v").alias("p")).filter("p > 1000"))
cronometrar("Funciones de Spark", datos.select(nativa.alias("p")).filter("p > 1000"))
