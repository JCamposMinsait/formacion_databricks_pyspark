# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2a · PySpark paso a paso
# MAGIC
# MAGIC Duración: 1 h 15 min aproximadamente.
# MAGIC
# MAGIC Con la telemetría del lunes vamos a filtrar, crear columnas, limpiar nulos, agrupar y unir con el maestro. Al final hacemos lo mismo en SQL.
# MAGIC
# MAGIC | En SQL | En PySpark |
# MAGIC |---|---|
# MAGIC | `SELECT a, b` | `df.select("a", "b")` |
# MAGIC | `WHERE a = 1` | `df.filter(F.col("a") == 1)` |
# MAGIC | `a * 2 AS doble` | `df.withColumn("doble", F.col("a") * 2)` |
# MAGIC | `CASE WHEN` | `F.when(...).otherwise(...)` |
# MAGIC | `GROUP BY a` | `df.groupBy("a").agg(...)` |
# MAGIC | `ORDER BY a DESC` | `df.orderBy(F.desc("a"))` |
# MAGIC | `JOIN b USING (id)` | `df.join(b, "id")` |
# MAGIC
# MAGIC En las primeras celdas de **Ahora tú** hay que completar lo que está marcado con `___`. Desde la sección 5 escribes tú el código entero. Los retos del final son opcionales y van de menos a más.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Ver la telemetría
# MAGIC Partimos de la tabla Bronze. La columna `ts` viene como texto y la convertimos a timestamp al leerla.

# COMMAND ----------

lecturas = spark.table(t("bronze_telemetria")).withColumn("ts", F.to_timestamp("ts"))
lecturas.printSchema()
display(lecturas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta cuántas lecturas hay en total y cuántas turbinas distintas.
# MAGIC
# MAGIC Deberían salir unos 3,5 millones de lecturas y 242 turbinas (dos de ellas no están en el maestro).

# COMMAND ----------

print("Lecturas:", lecturas.___())
print("Turbinas:", lecturas.select(___).distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Elegir columnas y filtrar
# MAGIC Las condiciones se combinan con `&` (y) y `|` (o), cada una entre paréntesis.

# COMMAND ----------

display(lecturas.filter((F.col("signal") == "power_kw") & (F.col("park_id") == "P03"))
        .select("turbine_id", "ts", "value").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Las lecturas de temperatura de la multiplicadora (`temp_gearbox_c`) por encima de 80 ºC, con turbina, instante y valor.
# MAGIC
# MAGIC Deberían salir sobre todo WTG-0013 y WTG-0187.

# COMMAND ----------

display(lecturas.filter((F.col("signal") == ___) & (F.col("value") > ___))
        .select(___, "ts", "value"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Columnas nuevas
# MAGIC `withColumn` crea o sustituye una columna. `F.when(...).otherwise(...)` es el `CASE WHEN` de SQL.

# COMMAND ----------

potencia = (lecturas.filter(F.col("signal") == "power_kw")
            .withColumn("potencia_mw", F.round(F.col("value") / 1000, 3))
            .withColumn("estado", F.when(F.col("value") > 3000, "ALTA").when(F.col("value") > 0, "BAJA").otherwise("PARADA")))
display(potencia.select("turbine_id", "ts", "value", "potencia_mw", "estado").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Sobre las lecturas de viento (`wind_speed`), una columna `zona` que sea `CALMA` por debajo de 3 m/s, `CORTE` desde 25 m/s y `OPERABLE` en el resto.
# MAGIC
# MAGIC Casi todas son OPERABLE y hay algunas CALMA. CORTE no aparece, porque en estos datos el viento nunca llega a 25 m/s.

# COMMAND ----------

viento = (lecturas.filter(F.col("signal") == "wind_speed")
          .withColumn("zona", F.when(F.col("value") < ___, "CALMA").when(F.col("value") >= ___, "CORTE").otherwise(___)))
display(viento.groupBy("zona").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Nulos y centinelas
# MAGIC Cuando el sensor no tiene lectura envía `-9999`. Lo pasamos a nulo para que no altere medias ni sumas. `coalesce` devuelve el primer valor no nulo.

# COMMAND ----------

limpio = lecturas.withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
print("Nulos después de quitar el -9999:", limpio.filter(F.col("value").isNull()).count())
display(limpio.select("value", F.coalesce("value", F.lit(0.0)).alias("value_o_cero")).filter(F.col("value").isNull()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Compara la media de `power_kw` con los `-9999` dentro y después de convertirlos en nulo.
# MAGIC
# MAGIC Con los -9999 dentro la media sale más baja.

# COMMAND ----------

sucia = lecturas.filter(F.col("signal") == "power_kw").agg(F.avg(___)).first()[0]
buena = limpio.filter(F.col("signal") == "power_kw").agg(F.avg("value")).first()[0]
print(f"Con -9999: {sucia:.1f} kW | Sin -9999: {buena:.1f} kW")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Agrupar
# MAGIC `groupBy` indica las columnas por las que se agrupa y `agg` lo que se calcula. Con `alias` se le da nombre al resultado.

# COMMAND ----------

display(limpio.filter(F.col("signal") == "power_kw").groupBy("park_id")
        .agg(F.round(F.avg("value"), 1).alias("kw_medio"), F.count("*").alias("lecturas"))
        .orderBy("park_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC La temperatura máxima de la multiplicadora de cada turbina, de más a menos. Quédate con las 5 primeras.
# MAGIC
# MAGIC A partir de aquí la celda viene vacía y escribes tú la consulta entera, siguiendo el ejemplo de arriba.
# MAGIC
# MAGIC Arriba deberían salir WTG-0013 y WTG-0187.

# COMMAND ----------

# Filtra la señal temp_gearbox_c, agrupa por turbina, saca el máximo, ordena de más a menos y quédate con 5


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Unir con el maestro
# MAGIC El maestro tiene el modelo y el parque de cada turbina. Si la clave se llama igual en las dos tablas, basta con pasar su nombre.

# COMMAND ----------

maestro = df_turbinas().join(df_parques().select("park_id", F.col("nombre").alias("parque"), "provincia"), "park_id")
display(limpio.filter(F.col("signal") == "power_kw")
        .join(maestro.select("turbine_id", "parque", "provincia"), "turbine_id")
        .groupBy("provincia").agg(F.round(F.avg("value"), 1).alias("kw_medio")).orderBy("provincia"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC La potencia media por modelo de turbina, de más a menos.
# MAGIC
# MAGIC Salen los tres modelos, y no tiene por qué ganar el de más potencia nominal.

# COMMAND ----------

# Filtra power_kw, une con el maestro (turbine_id y modelo), agrupa por modelo, saca la media y ordena


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lo mismo en SQL
# MAGIC Con una vista temporal se puede consultar un DataFrame desde SQL. Por debajo el motor es el mismo, así que se usa el que resulte más claro.

# COMMAND ----------

limpio.createOrReplaceTempView("lecturas_v")
maestro.createOrReplaceTempView("maestro_v")
display(spark.sql("""
    SELECT m.provincia, round(avg(l.value), 1) AS kw_medio
    FROM lecturas_v l JOIN maestro_v m USING (turbine_id)
    WHERE l.signal = 'power_kw'
    GROUP BY m.provincia ORDER BY m.provincia"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC En una celda `%sql`, la temperatura máxima por parque usando `lecturas_v`.
# MAGIC
# MAGIC Sale una fila por parque. Arriba quedan P01 y P06, los parques de WTG-0013 y WTG-0187.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Temperatura máxima de la multiplicadora por parque, de más a menos, sobre lecturas_v

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC Van de menos a más. El último es para quien vaya sobrado.
# MAGIC
# MAGIC 1. Las 5 turbinas con más horas de viento por encima de 12 m/s. Cada lectura es un minuto, así que cuenta y divide entre 60.
# MAGIC 2. Las lecturas de turbinas que no están en el maestro, con un `left_anti`.
# MAGIC 3. El viento medio por hora del día en el parque P03. ¿A qué hora sopla más? La hora se saca con `F.hour("ts")`.
# MAGIC 4. Un resumen por provincia con la energía producida en MWh, el número de turbinas y la temperatura máxima de la multiplicadora, todo en un solo `agg`. Para quedarte con una señal dentro de una agregación, mete un `F.when` dentro, por ejemplo `F.max(F.when(F.col("signal") == "temp_gearbox_c", F.col("value")))`. La energía es la suma de `power_kw` entre 60, porque cada lectura es un minuto, y entre 1000 para pasar de kWh a MWh.
# MAGIC 5. ¿Qué turbinas producen menos que sus vecinas? Las turbinas del mismo parque y del mismo modelo reciben casi el mismo viento, así que deberían producir parecido. Hazlo en tres pasos:
# MAGIC    - La potencia media de cada turbina, con su parque y su modelo. Usa solo las lecturas de `power_kw` mayores que 0, para no contar las paradas, y une con el maestro para tener el modelo.
# MAGIC    - Con ese resultado, la potencia media de cada parque y modelo.
# MAGIC    - Une los dos por parque y modelo con `join(otro, ["park_id", "modelo"])`, divide la media de la turbina entre la de su grupo y ordena de menor a mayor.
# MAGIC
# MAGIC    Abajo deberían quedar WTG-0042, WTG-0117 y WTG-0199, en torno a 0,8.
