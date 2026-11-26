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
# MAGIC En las celdas de **Ahora tú** hay que completar lo que está marcado con `___`. En la última escribes tú el código entero. Los retos del final son opcionales y van de menos a más.

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
# MAGIC Hasta ahora `repartition(n)` repartía las filas sin mirar lo que tienen. Si además le pasas una columna, `repartition(n, "columna")`, todas las filas con el mismo valor van a la misma partición. Es lo que hace Spark por dentro en un `groupBy` o en un `join` por esa columna: junta en un sitio todo lo de cada clave.
# MAGIC
# MAGIC Reparte las lecturas por `park_id` y cuenta las filas de cada partición. Tú eliges cuántas particiones: piensa cuántos parques hay (`lecturas.select("park_id").distinct().count()`) y qué número tendría sentido.
# MAGIC
# MAGIC Después mira el resultado. ¿Sale una partición por parque? ¿Salen parejas? ¿Por qué una tiene tantas más filas que las demás? Acuérdate de cómo se reparten las turbinas en el maestro.

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(___, ___)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Skew y broadcast
# MAGIC Lo que acabas de ver se llama skew: una clave con muchas más filas que las demás. En un `groupBy` o un `join` por `park_id`, Spark reparte justo así, y la tarea que se queda con P01 hace casi todo el trabajo mientras las demás esperan.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Lo primero que se le ocurre a cualquiera es poner más particiones, para que cada una tenga menos trabajo. Repite el reparto por parque con 16 particiones.
# MAGIC
# MAGIC ¿Mejora la partición de P01? ¿Por qué crees que pasa?

# COMMAND ----------

display(filas_por_particion(lecturas.repartition(___, "park_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC En un join con una tabla pequeña hay una salida mejor: el broadcast. La tabla pequeña se copia entera a cada máquina y la grande no se reparte por la clave, así que da igual lo desequilibrada que esté.
# MAGIC
# MAGIC Compara los dos planes. Con sort-merge hay un Exchange por `park_id` en los dos lados. Con broadcast, las lecturas no tienen Exchange.

# COMMAND ----------

tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")
lecturas.join(tarifas.hint("merge"), "park_id").explain()       # sort-merge: Exchange por park_id en los dos lados

# COMMAND ----------

lecturas.join(F.broadcast(tarifas), "park_id").explain()         # broadcast: sin Exchange en las lecturas

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
# MAGIC Escribe lo mismo con `F.when` y mide el tiempo. Esta vez escribes tú la celda entera, siguiendo la de la UDF.
# MAGIC
# MAGIC Sale el mismo recuento salvo los nulos: la UDF los deja en nulo y `F.when` los manda a OPERABLE. Normalmente tarda menos que la UDF.

# COMMAND ----------

# Guarda la hora en t0, crea la columna zona con F.when sobre viento, agrupa y cuenta con collect() y escribe lo que ha tardado


# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC Van de menos a más. El último es para quien vaya sobrado.
# MAGIC
# MAGIC 1. ¿En cuántas particiones llega la Bronze tal cual la lees y cuántas filas trae cada una? Usa `filas_por_particion` sin repartir antes.
# MAGIC 2. El skew se arregla con *salting*. A cada fila se le añade un número al azar del 0 al 7, la sal. Así P01 deja de ser una sola clave y pasa a ser ocho (P01 con sal 0, P01 con sal 1...), y cada una puede ir a una partición distinta. La columna se crea con `con_sal = lecturas.withColumn("sal", (F.rand() * 8).cast("int"))`. Reparte `con_sal` por `park_id` y `sal` con `repartition(8, "park_id", "sal")` y compara el reparto con el de solo `park_id`.
# MAGIC 3. La sal reparte el trabajo, pero el resultado tiene que ser el mismo. Cuenta las lecturas de cada parque de dos formas y compara: directamente con `con_sal.groupBy("park_id").count()`, y en dos pasos, primero `groupBy("park_id", "sal").count()` y después sumando esos recuentos por parque.
# MAGIC 4. Escribe una Pandas UDF para la zona de viento y compárala en tiempo con la UDF normal. Se declara con `@pandas_udf("string")` y recibe una `pd.Series` entera en lugar de un valor suelto.
# MAGIC 5. Escribe una función `resumen_particiones(df)` que devuelva en una fila el número de particiones con datos, las filas de la mayor, las de la menor y cuántas veces es mayor una que otra. Úsala para comparar el reparto por parque, el reparto por parque y sal y el `coalesce(2)`.
