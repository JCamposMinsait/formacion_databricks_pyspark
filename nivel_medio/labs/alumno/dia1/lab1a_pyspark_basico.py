# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1a · Puesta a punto de PySpark
# MAGIC
# MAGIC Duración: 45 min aproximadamente.
# MAGIC
# MAGIC Con PySpark escribes en Python lo mismo que harías en SQL. Si sabes SQL ya tienes casi todo, solo cambia la forma de escribirlo. Cada paso tiene un ejemplo resuelto y un ejercicio parecido.
# MAGIC
# MAGIC | En SQL | En PySpark |
# MAGIC |---|---|
# MAGIC | `SELECT a, b` | `df.select("a", "b")` |
# MAGIC | `WHERE a = 1` | `df.filter(F.col("a") == 1)` |
# MAGIC | `a * 2 AS doble` | `df.withColumn("doble", F.col("a") * 2)` |
# MAGIC | `CASE WHEN ... THEN ... ELSE ... END` | `F.when(...).otherwise(...)` |
# MAGIC | `GROUP BY a` + `count(*)` | `df.groupBy("a").agg(F.count("*"))` |
# MAGIC | `ORDER BY a DESC` | `df.orderBy(F.desc("a"))` |
# MAGIC | `JOIN b USING (id)` | `df.join(b, "id")` |
# MAGIC
# MAGIC > `F` son las funciones de Spark (`from pyspark.sql import functions as F`). Ya vienen importadas en la configuración.
# MAGIC
# MAGIC En los **ejercicios** hay que completar lo que está marcado con `___`. Los retos del final son opcionales.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Ver un DataFrame
# MAGIC Un DataFrame es una tabla. `display` la enseña, `printSchema` dice las columnas y sus tipos y `count` cuenta las filas.

# COMMAND ----------

parques = df_parques()
display(parques)
parques.printSchema()
print("Filas:", parques.count(), "| Columnas:", parques.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** haz lo mismo con las turbinas, que salen de `df_turbinas()`.

# COMMAND ----------

turbinas = ___
display(___)
turbinas.___()
print("Filas:", turbinas.___())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Elegir columnas y filtrar (SELECT y WHERE)
# MAGIC Las condiciones se combinan con `&` (y), `|` (o) y `~` (no), cada una entre paréntesis.

# COMMAND ----------

display(parques.select("nombre", "provincia"))
display(parques.filter(F.col("operador") == "EnerSur"))
display(parques.filter((F.col("lat") > 40) & (F.col("operador") != "EnerSur")).select("nombre", "lat"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** las turbinas del modelo `BX-4500` que están en el parque `P01`, solo con `turbine_id` y `potencia_nominal_kw`.

# COMMAND ----------

display(turbinas.filter((F.col("modelo") == ___) & (___ == "P01"))
                .select(___, ___))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Columnas nuevas (AS y CASE WHEN)
# MAGIC `withColumn` añade una columna o sustituye una que ya existe. `F.when(...).otherwise(...)` es el `CASE WHEN` de SQL.

# COMMAND ----------

display(turbinas
        .withColumn("potencia_mw", F.col("potencia_nominal_kw") / 1000)
        .withColumn("tamano", F.when(F.col("diametro_rotor_m") >= 149, "grande").otherwise("normal")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** añade `potencia_mw` redondeada a un decimal (con `F.round`) y una columna `en_huelva` que sea `true` si el parque es `P01`.

# COMMAND ----------

display(turbinas
        .withColumn("potencia_mw", F.round(___, 1))
        .withColumn("en_huelva", ___))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Agrupar (GROUP BY)
# MAGIC `groupBy` indica las columnas por las que se agrupa y `agg` lo que se calcula. Con `alias` se le da nombre al resultado, igual que con el `AS` de SQL.

# COMMAND ----------

display(turbinas.groupBy("park_id")
        .agg(F.count("*").alias("turbinas"), F.sum("potencia_nominal_kw").alias("potencia_total_kw"))
        .orderBy("park_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** por modelo, cuántas turbinas hay y su potencia media, ordenado de más a menos turbinas (con `F.desc`).

# COMMAND ----------

display(turbinas.groupBy(___)
        .agg(F.count("*").alias("turbinas"), F.___("potencia_nominal_kw").alias("potencia_media_kw"))
        .orderBy(F.desc(___)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Unir tablas (JOIN)
# MAGIC Si la clave se llama igual en las dos tablas, basta con pasar su nombre y no sale repetida. Para un `LEFT JOIN` se añade `"left"` como tercer argumento.

# COMMAND ----------

display(turbinas.join(parques, "park_id")
        .groupBy("provincia").agg(F.count("*").alias("turbinas")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** la potencia total en MW de cada operador.

# COMMAND ----------

display(turbinas.join(parques, ___)
        .groupBy(___).agg(F.round(F.sum("potencia_nominal_kw") / 1000, 1).alias("potencia_mw")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Lecturas de sensores
# MAGIC Generamos lecturas de prueba de 10 turbinas durante un día, una por minuto, con el viento y la temperatura de la multiplicadora.

# COMMAND ----------

lecturas = (spark.range(0, 24 * 60).withColumnRenamed("id", "minuto")
    .crossJoin(turbinas.select("turbine_id").limit(10))
    .withColumn("hora", (F.col("minuto") / 60).cast("int"))
    .withColumn("viento", F.round(F.rand(1) * 20, 1))
    .withColumn("temp_c", F.round(50 + F.rand(2) * 35, 1)))
print("Lecturas:", lecturas.count())

# Ejemplo: temperatura máxima por turbina
display(lecturas.groupBy("turbine_id").agg(F.max("temp_c").alias("temp_max")).orderBy(F.desc("temp_max")))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** por parque, el viento medio y la temperatura máxima, contando solo los minutos con viento de 3 m/s o más, ordenado de más a menos temperatura. Las lecturas no traen el parque, así que hay que unirlas con las turbinas.

# COMMAND ----------

display(lecturas.filter(___)
        .join(turbinas.select("turbine_id", "park_id"), ___)
        .groupBy(___)
        .agg(F.round(F.avg("viento"), 1).alias("viento_medio"), F.max("temp_c").alias("temp_max"))
        .orderBy(___))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lo mismo en SQL
# MAGIC Con una vista temporal se puede consultar un DataFrame desde SQL, con `spark.sql` o en una celda `%sql`. Por debajo el motor es el mismo, así que usa el que te resulte más claro.

# COMMAND ----------

lecturas.createOrReplaceTempView("lecturas_v")
turbinas.createOrReplaceTempView("turbinas_v")
display(spark.sql("""
    SELECT t.park_id, round(avg(l.viento), 1) AS viento_medio, max(l.temp_c) AS temp_max
    FROM lecturas_v l JOIN turbinas_v t USING (turbine_id)
    WHERE l.viento >= 3
    GROUP BY t.park_id
    ORDER BY temp_max DESC"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Ejercicio:** en una celda `%sql`, cuántas lecturas hay por hora con más de 80 ºC.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: lecturas con más de 80 ºC por hora
# MAGIC SELECT hora, count(*) AS lecturas_calientes
# MAGIC FROM ___
# MAGIC WHERE ___
# MAGIC GROUP BY hora
# MAGIC ORDER BY hora

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Guardar y leer una tabla
# MAGIC `saveAsTable` guarda el DataFrame como tabla Delta en tu esquema y `spark.table` la lee. Luego puedes buscarla en Catalog Explorer. Delta lo vemos a fondo en el lab 1b.

# COMMAND ----------

resumen = (turbinas.join(parques, "park_id")
    .groupBy(F.col("nombre").alias("parque"), "operador")
    .agg(F.count("*").alias("turbinas"), F.round(F.sum("potencia_nominal_kw") / 1000, 1).alias("potencia_mw")))
resumen.write.mode("overwrite").saveAsTable(t("pyspark_resumen_parques"))
display(spark.table(t("pyspark_resumen_parques")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC 1. Las 5 turbinas con más horas de viento por encima de 12 m/s (cuenta los minutos y divide entre 60).
# MAGIC 2. ¿Cuántos parques tiene cada operador? Hazlo en PySpark y en SQL.
# MAGIC 3. Añade a las lecturas una columna `alerta`: `ALTA` si la temperatura pasa de 80, `MEDIA` si pasa de 70 y `NORMAL` en el resto. Cuenta cuántas hay de cada una.
