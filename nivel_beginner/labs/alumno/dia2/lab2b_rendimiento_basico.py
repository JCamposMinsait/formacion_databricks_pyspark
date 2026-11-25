# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2b · Rendimiento básico
# MAGIC
# MAGIC Duración: 45 min aproximadamente.
# MAGIC
# MAGIC Vamos a ver con nuestros datos lo que contamos en la teoría sobre las operaciones que mueven datos, el reparto en particiones, el skew y las UDFs frente a las funciones de Spark.
# MAGIC
# MAGIC > En la cuenta gratuita no hay Spark UI. Lo vemos con `explain`, contando filas por partición y con el Query Profile (**See performance** debajo de cada resultado).
# MAGIC
# MAGIC En las celdas de **Ahora tú** hay que completar lo que está marcado con `___`. Los retos del final son opcionales.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

import time

lecturas = spark.table(t("bronze_telemetria"))


def filas_por_particion(df):
    return df.groupBy(F.spark_partition_id().alias("particion")).count().orderBy("particion")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Narrow y wide
# MAGIC En el plan, un **Exchange** es un shuffle, es decir, datos que viajan entre máquinas. Un filtro no lo necesita y un groupBy sí.

# COMMAND ----------

lecturas.filter(F.col("signal") == "power_kw").explain()     # narrow: sin Exchange

# COMMAND ----------

lecturas.groupBy("turbine_id").count().explain()             # wide: con Exchange

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Mira el plan de un join de las lecturas con el maestro de turbinas. ¿Qué tipo de join sale?
# MAGIC
# MAGIC Lo normal es que salga un BroadcastHashJoin. El maestro es pequeño y Spark lo copia a todas las máquinas sin que se lo pidas.

# COMMAND ----------

lecturas.join(df_turbinas(), ___).___()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Particiones
# MAGIC `repartition` vuelve a repartir con un shuffle completo. `coalesce` junta particiones sin repartir y solo sirve para reducir su número.

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(8)))               # 8 particiones parejas

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(8).coalesce(2)))   # 2 particiones, sin shuffle

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Reparte las lecturas en 6 particiones **por parque** (`repartition(6, "park_id")`) y cuenta filas por partición.
# MAGIC
# MAGIC Una partición, la del parque P01, se lleva el 60 % de las filas. Eso es skew.

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(___, ___)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Skew y broadcast
# MAGIC Si se reparte por una clave desequilibrada, una sola tarea hace casi todo el trabajo. Poner más particiones no lo arregla, porque las filas de una misma clave van siempre juntas. El broadcast sí lo arregla, porque la tabla grande no se mueve.

# COMMAND ----------

tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")
lecturas.join(tarifas.hint("merge"), "park_id").explain()       # sort-merge: Exchange por park_id en los dos lados

# COMMAND ----------

lecturas.join(F.broadcast(tarifas), "park_id").explain()         # broadcast: sin Exchange en las lecturas

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Repite el reparto por parque, pero con 16 particiones. ¿Mejora P01?
# MAGIC
# MAGIC No mejora, P01 sigue entero en una sola partición.

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(___, "park_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · UDF o función de Spark
# MAGIC Una UDF es una función tuya en Python que Spark aplica fila a fila. Es flexible, pero Spark no la puede optimizar. Aquí comparamos una UDF con lo mismo escrito con funciones de Spark.

# COMMAND ----------

from pyspark.sql.functions import udf


def zona_viento(v):
    if v is None:
        return None
    return "CALMA" if v < 3 else ("CORTE" if v >= 25 else "OPERABLE")


zona_udf = udf(zona_viento, "string")
viento = lecturas.filter(F.col("signal") == "wind_speed")
t0 = time.time()
viento.select(zona_udf("value").alias("zona")).groupBy("zona").count().collect()
print(f"Con UDF: {time.time() - t0:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Escribe lo mismo con `F.when` y mide el tiempo.
# MAGIC
# MAGIC Sale el mismo recuento y normalmente tarda menos que la UDF.

# COMMAND ----------

t0 = time.time()
zona = F.when(F.col("value") < ___, "CALMA").when(F.col("value") >= ___, "CORTE").otherwise(___)
viento.select(zona.alias("zona")).groupBy("zona").count().collect()
print(f"Con funciones de Spark: {time.time() - t0:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. Arregla el skew con *salting*: añade una columna `sal` aleatoria de 0 a 7 y reparte por `park_id` y `sal`.
# MAGIC 2. Haz la suma de potencia por parque en dos fases con la sal: primero por `park_id` y `sal`, después solo por `park_id`. Comprueba que da lo mismo.
# MAGIC 3. Escribe una Pandas UDF para la zona de viento y compárala en tiempo con la UDF normal.
