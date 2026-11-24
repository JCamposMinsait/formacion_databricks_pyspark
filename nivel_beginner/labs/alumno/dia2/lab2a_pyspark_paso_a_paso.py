# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 2a · PySpark paso a paso (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~1 h 15 min
# MAGIC
# MAGIC Trabajas con la telemetría del lunes: ver, filtrar, crear columnas, limpiar nulos, agrupar, unir con el maestro y hacer lo mismo en SQL.
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
# MAGIC > **Cómo seguir esta guía**
# MAGIC > - Va paso a paso: lee la explicación, ejecuta la celda y mira el resultado. El código de los pasos difíciles ya está escrito.
# MAGIC > - En los ejercicios **Ahora tú** te toca escribir a ti: rellena lo marcado con `___`. Si te atascas, mira el código de justo encima.
# MAGIC > - Los retos del final son para quien vaya rápido.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Ver la telemetría
# MAGIC Partimos de la Bronze. `ts` llega como texto, así que lo pasamos a fecha y hora nada más leer.

# COMMAND ----------

lecturas = spark.table(t("bronze_telemetria")).withColumn("ts", F.to_timestamp("ts"))
lecturas.printSchema()
display(lecturas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta cuántas lecturas hay en total y cuántas turbinas distintas.
# MAGIC
# MAGIC **Comprueba:** unos 3,5 millones de lecturas y 242 turbinas: las 240 del maestro y dos que no existen.

# COMMAND ----------

print("Lecturas:", lecturas.___())
print("Turbinas:", lecturas.select(___).distinct().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Elegir columnas y filtrar
# MAGIC Para combinar condiciones: `&` es y, `|` es o. Cada condición va entre paréntesis.

# COMMAND ----------

display(lecturas.filter((F.col("signal") == "power_kw") & (F.col("park_id") == "P03"))
        .select("turbine_id", "ts", "value").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Las lecturas de temperatura de la multiplicadora (`temp_gearbox_c`) por encima de 80 ºC, con turbina, instante y valor.
# MAGIC
# MAGIC **Comprueba:** salen sobre todo WTG-0013 y WTG-0187, las dos que se calientan.

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
# MAGIC **Comprueba:** casi todas salen OPERABLE y unas pocas CALMA. CORTE no sale casi nunca.

# COMMAND ----------

viento = (lecturas.filter(F.col("signal") == "wind_speed")
          .withColumn("zona", F.when(F.col("value") < ___, "CALMA").when(F.col("value") >= ___, "CORTE").otherwise(___)))
display(viento.groupBy("zona").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Nulos y centinelas
# MAGIC El `-9999` es un valor centinela: el sensor no tenía lectura. Se convierte en nulo para que no estropee medias ni sumas. `coalesce` devuelve el primer valor que no sea nulo.

# COMMAND ----------

limpio = lecturas.withColumn("value", F.when(F.col("value") != -9999, F.col("value")))
print("Nulos después de quitar el -9999:", limpio.filter(F.col("value").isNull()).count())
display(limpio.select("value", F.coalesce("value", F.lit(0.0)).alias("value_o_cero")).filter(F.col("value").isNull()).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Compara la media de `power_kw` con los `-9999` dentro y después de convertirlos en nulo.
# MAGIC
# MAGIC **Comprueba:** con los -9999 dentro la media sale más baja: unos pocos valores absurdos bastan para desviarla. Por eso se limpian.

# COMMAND ----------

sucia = lecturas.filter(F.col("signal") == "power_kw").agg(F.avg(___)).first()[0]
buena = limpio.filter(F.col("signal") == "power_kw").agg(F.avg("value")).first()[0]
print(f"Con -9999: {sucia:.1f} kW | Sin -9999: {buena:.1f} kW")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Agrupar
# MAGIC `groupBy` dice por qué se agrupa y `agg` qué se calcula. `alias` le pone nombre al resultado.

# COMMAND ----------

display(limpio.filter(F.col("signal") == "power_kw").groupBy("park_id")
        .agg(F.round(F.avg("value"), 1).alias("kw_medio"), F.count("*").alias("lecturas"))
        .orderBy("park_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC La temperatura máxima de la multiplicadora de cada turbina, de más a menos. Quédate con las 5 primeras.
# MAGIC
# MAGIC **Comprueba:** arriba salen WTG-0013 y WTG-0187.

# COMMAND ----------

display(limpio.filter(F.col("signal") == "temp_gearbox_c").groupBy(___)
        .agg(F.max(___).alias("temp_max")).orderBy(F.desc(___)).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Unir con el maestro
# MAGIC El maestro tiene el modelo y el parque de cada turbina. Si la clave se llama igual en las dos tablas, se pasa su nombre como texto.

# COMMAND ----------

maestro = df_turbinas().join(df_parques().select("park_id", F.col("nombre").alias("parque"), "provincia"), "park_id")
display(limpio.filter(F.col("signal") == "power_kw")
        .join(maestro.select("turbine_id", "parque", "provincia"), "turbine_id")
        .groupBy("provincia").agg(F.round(F.avg("value"), 1).alias("kw_medio")).orderBy("provincia"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC La potencia media por modelo de turbina.
# MAGIC
# MAGIC **Comprueba:** salen los tres modelos. El que tiene más potencia nominal no tiene por qué salir el primero.

# COMMAND ----------

display(limpio.filter(F.col("signal") == "power_kw")
        .join(maestro.select("turbine_id", ___), "turbine_id")
        .groupBy(___).agg(F.round(F.avg("value"), 1).alias("kw_medio")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lo mismo en SQL
# MAGIC Una vista temporal deja usar un DataFrame desde SQL. PySpark y SQL usan el mismo motor: se elige lo que mejor se lea.

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
# MAGIC **Comprueba:** sale una fila por parque. El que tiene WTG-0013 y WTG-0187 está arriba.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: temperatura máxima de la multiplicadora por parque
# MAGIC SELECT park_id, max(value) AS temp_max
# MAGIC FROM ___
# MAGIC WHERE signal = ___
# MAGIC GROUP BY park_id
# MAGIC ORDER BY temp_max DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para ir más allá (avanzado)
# MAGIC 1. Las 5 turbinas con más horas de viento por encima de 12 m/s (cuenta minutos y divide entre 60).
# MAGIC 2. Las lecturas de turbinas que no están en el maestro, con un `left_anti`.
# MAGIC 3. La potencia media por hora del día de todo el parque P03. ¿A qué hora sopla más?
