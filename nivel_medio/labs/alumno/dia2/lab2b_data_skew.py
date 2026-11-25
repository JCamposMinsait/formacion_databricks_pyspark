# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2b · Data skew sin Spark UI
# MAGIC
# MAGIC Duración: 30 min aproximadamente.
# MAGIC
# MAGIC En Free Edition solo hay cómputo serverless, así que no hay Spark UI. El skew lo vamos a ver contando cuántas filas caen en cada partición, mirando los planes con `explain` y abriendo el Query Profile. En un clúster clásico, lo mismo se ve en el Spark UI, en la pestaña Stages, comparando la duración máxima de las tareas con la mediana.
# MAGIC
# MAGIC El caso: un join entre las lecturas y una tabla de tarifas por parque, con el parque P01 llevándose el 70 % de las filas.
# MAGIC
# MAGIC En las celdas hay que completar lo que está marcado con `___` o `# TODO`. Los retos del final son opcionales.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Datos desequilibrados
# MAGIC Generamos cinco millones de lecturas, el 70 % del parque P01, y una tabla de tarifas con seis filas. Esta celda solo hay que ejecutarla.

# COMMAND ----------

import time

N_FILAS = 5_000_000
grande = (spark.range(N_FILAS)
          .withColumn("park_id", F.when(F.rand(1) < 0.7, "P01")
                      .otherwise(F.concat(F.lit("P0"), (F.rand(2) * 5 + 2).cast("int").cast("string"))))
          .withColumn("valor", F.rand(3) * 100))
tarifas = spark.createDataFrame([(f"P0{i}", 50.0 + i) for i in range(1, 7)], "park_id string, precio double")

display(grande.groupBy("park_id").count().orderBy("park_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Ver el skew: filas por partición
# MAGIC Reparte las filas por `park_id` en 8 particiones y cuenta cuántas caen en cada una.
# MAGIC - `repartition(n, "columna")` reparte por el valor de la columna.
# MAGIC - `F.spark_partition_id()` dice en qué partición está cada fila.
# MAGIC - En el resultado, la pestaña de gráfico enseña el reparto de un vistazo.
# MAGIC
# MAGIC Lo tienes en la slide Día 2 · Data skew: cuando una tarea hace todo el trabajo.

# COMMAND ----------

# TODO: reparte por park_id en 8 particiones y cuenta las filas de cada partición
reparto = (grande.repartition(8, ___)
           .groupBy(___.alias("particion")).count()
           .orderBy("particion"))
display(reparto)

# COMMAND ----------

# MAGIC %md
# MAGIC Hay 8 particiones. ¿Cuántas tienen datos? ¿Qué tarea tardaría más en el Spark UI?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Join con sort-merge
# MAGIC Forzamos un sort-merge join, miramos su plan y lo medimos.
# MAGIC - Si no se fuerza, Spark hace broadcast solo porque `tarifas` es pequeña. El hint `"merge"` fuerza el sort-merge.
# MAGIC - Después de ejecutar, abre **See performance** debajo del resultado para ver el Query Profile.
# MAGIC - En serverless, AQE está siempre activo y puede partir la partición grande, así que el tiempo puede no salir tan malo. Lo que importa aquí es entender el reparto.
# MAGIC
# MAGIC Lo tienes en la slide Día 2 · Cómo ejecuta Spark un join.

# COMMAND ----------

def medir(df, nombre):
    t = time.time()
    r = df.groupBy("park_id").agg(F.sum("importe").alias("importe")).orderBy("park_id").collect()
    print(f"{nombre}: {time.time() - t:.1f} s")
    return r


# TODO: fuerza el sort-merge con un hint sobre tarifas
smj = grande.join(tarifas.hint(___), "park_id").withColumn("importe", F.col("valor") * F.col("precio"))
smj.explain()   # busca los Exchange y el SortMergeJoin
r_smj = medir(smj, "sort-merge")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Broadcast
# MAGIC Hacemos el mismo join copiando la tabla pequeña a todas las máquinas.
# MAGIC - `F.broadcast(df)` marca la tabla que se copia.
# MAGIC - En el plan tiene que salir BroadcastHashJoin y ningún Exchange en el lado grande.
# MAGIC
# MAGIC Lo tienes en la slide Día 2 · Cómo se arregla el skew, en este orden.

# COMMAND ----------

# TODO: el mismo join con broadcast
bc = grande.join(___, "park_id").withColumn("importe", F.col("valor") * F.col("precio"))
bc.explain()
r_bc = medir(bc, "broadcast")

# Y sin ningún hint, ¿qué elige Spark?
grande.join(tarifas, "park_id").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Salting
# MAGIC Repartimos P01 en varias particiones añadiendo una sal a la clave.
# MAGIC - En el lado grande, la sal es un número aleatorio entre 0 y N_SAL - 1.
# MAGIC - El lado pequeño se replica una vez por cada valor de la sal, con un `crossJoin`.
# MAGIC - El join pasa a hacerse por `park_id` y `sal`.
# MAGIC
# MAGIC Lo tienes en la slide Día 2 · Cómo se arregla el skew, en este orden.

# COMMAND ----------

N_SAL = 8
# TODO: añade la columna sal al lado grande
grande_sal = grande.withColumn("sal", ___)
tarifas_sal = tarifas.crossJoin(spark.range(N_SAL).select(F.col("id").cast("int").alias("sal")))

display(grande_sal.repartition(8, "park_id", "sal")
        .groupBy(F.spark_partition_id().alias("particion")).count()
        .orderBy("particion"))   # ¿cómo es ahora el reparto?

# TODO: el join por las dos columnas
sal = (grande_sal.join(tarifas_sal.hint("merge"), ___)
       .withColumn("importe", F.col("valor") * F.col("precio")))
r_sal = medir(sal, "salting")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · El resultado es el mismo
# MAGIC
# MAGIC Las tres versiones tienen que dar exactamente lo mismo. Lo único que cambia es cómo se reparte el trabajo.

# COMMAND ----------

def iguales(a, b):
    return all(x.park_id == y.park_id and abs(x.importe - y.importe) <= 1e-6 * abs(x.importe) for x, y in zip(a, b))

assert iguales(r_smj, r_bc) and iguales(r_smj, r_sal)
print("Los tres resultados son iguales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. El skew también afecta a las agregaciones. Haz la suma de `valor` por parque en dos fases con sal: primero por `park_id` y `sal`, y después solo por `park_id`. Comprueba que da lo mismo que en una sola fase.
# MAGIC 2. Cambia `N_SAL` a 2 y a 32 y mira el reparto. ¿Qué pasa con el tamaño de `tarifas_sal`?
# MAGIC 3. Pon el 30 % de los `park_id` a nulo y mira el reparto. ¿Dónde acaban todas las filas con clave nula?
