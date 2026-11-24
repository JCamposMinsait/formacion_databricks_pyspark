# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2b · Rendimiento básico (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~45 min
# MAGIC
# MAGIC Ves con tus datos lo que contamos en teoría: qué operaciones mueven datos, cómo se reparten las particiones, qué es el skew y por qué las funciones de Spark ganan a las UDFs.
# MAGIC
# MAGIC > En la cuenta gratuita no hay Spark UI. Lo vemos con `explain`, contando filas por partición y con el Query Profile (**See performance** debajo de cada resultado).
# MAGIC
# MAGIC > **Cómo seguir esta guía**
# MAGIC > - Va paso a paso: lee la explicación, ejecuta la celda y mira el resultado. El código de los pasos difíciles ya está escrito.
# MAGIC > - En los ejercicios **Ahora tú** te toca escribir a ti: rellena lo marcado con `___`. Si te atascas, mira el código de justo encima.
# MAGIC > - Los retos del final son para quien vaya rápido.

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
# MAGIC En el plan, un **Exchange** es un shuffle: los datos viajan entre máquinas. Un filtro no lo necesita, un groupBy sí.

# COMMAND ----------

lecturas.filter(F.col("signal") == "power_kw").explain()     # narrow: sin Exchange

# COMMAND ----------

lecturas.groupBy("turbine_id").count().explain()             # wide: con Exchange

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Mira el plan de un join de las lecturas con el maestro de turbinas. ¿Qué tipo de join sale?
# MAGIC
# MAGIC **Comprueba:** normalmente sale un BroadcastHashJoin: el maestro es pequeño y Spark lo copia a todas las máquinas sin que se lo pidas.

# COMMAND ----------

lecturas.join(df_turbinas(), ___).___()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Particiones
# MAGIC `repartition` reparte de nuevo con un shuffle completo. `coalesce` junta particiones sin repartir, y solo sirve para bajar.

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(8)))               # 8 particiones parejas

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(8).coalesce(2)))   # 2 particiones, sin shuffle

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Reparte las lecturas en 6 particiones **por parque** (`repartition(6, "park_id")`) y cuenta filas por partición.
# MAGIC
# MAGIC **Comprueba:** una partición se lleva el 60 % de las filas: la del parque P01. Eso es skew.

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(___, ___)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Skew y broadcast
# MAGIC Si se reparte por una clave desequilibrada, una tarea hace casi todo el trabajo. Más particiones no lo arreglan, porque las filas de una clave van siempre juntas. El broadcast sí, porque la tabla grande no se mueve.

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
# MAGIC **Comprueba:** no: P01 sigue entero en una sola partición. Más particiones no arreglan el skew.

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
# MAGIC **Comprueba:** sale el mismo recuento y, normalmente, en menos tiempo que con la UDF.

# COMMAND ----------

t0 = time.time()
zona = F.when(F.col("value") < ___, "CALMA").when(F.col("value") >= ___, "CORTE").otherwise(___)
viento.select(zona.alias("zona")).groupBy("zona").count().collect()
print(f"Con funciones de Spark: {time.time() - t0:.1f} s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para ir más allá (avanzado)
# MAGIC 1. Arregla el skew con *salting*: añade una columna `sal` aleatoria de 0 a 7 y reparte por `park_id` y `sal`.
# MAGIC 2. Haz la suma de potencia por parque en dos fases con la sal: primero por `park_id` y `sal`, después solo por `park_id`. Comprueba que da lo mismo.
# MAGIC 3. Escribe una Pandas UDF para la zona de viento y compárala en tiempo con la UDF normal.
