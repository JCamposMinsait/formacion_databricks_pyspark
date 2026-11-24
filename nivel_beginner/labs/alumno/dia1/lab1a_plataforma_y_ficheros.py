# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1a · La plataforma y la primera tabla (guía del alumno)
# MAGIC
# MAGIC **Duración:** ~45 min
# MAGIC
# MAGIC Te mueves por la plataforma, subes ficheros a un volume, los lees con Spark y creas tu primera tabla.
# MAGIC
# MAGIC > Lo primero: elige **Serverless** arriba a la derecha y ejecuta la celda de configuración. Te crea tu esquema personal, `eolica_<tu usuario>`, para que nadie pise a nadie.
# MAGIC
# MAGIC > **Cómo seguir esta guía**
# MAGIC > - Va paso a paso: lee la explicación, ejecuta la celda y mira el resultado. El código de los pasos difíciles ya está escrito.
# MAGIC > - En los ejercicios **Ahora tú** te toca escribir a ti: rellena lo marcado con `___`. Si te atascas, mira el código de justo encima.
# MAGIC > - Los retos del final son para quien vaya rápido.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · El notebook
# MAGIC Cada celda puede ir en su lenguaje: Python por defecto, `%sql` para SQL y `%md` para texto. Los widgets son los parámetros del notebook: aparecen arriba como cuadros.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS yo, current_catalog() AS catalogo, current_schema() AS esquema

# COMMAND ----------

dbutils.widgets.text("parque", "P01")
parque = dbutils.widgets.get("parque")
print("Parque elegido:", parque)      # cambia el valor en el cuadro de arriba y vuelve a ejecutar

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Crea un widget `operador` con el valor `EnerSur` y escribe su valor.
# MAGIC
# MAGIC **Comprueba:** arriba aparece un cuadro nuevo y la celda escribe EnerSur. Si lo cambias y vuelves a ejecutar, cambia también.

# COMMAND ----------

dbutils.widgets.text(___, ___)
operador = dbutils.widgets.get(___)
print("Operador elegido:", operador)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El catálogo
# MAGIC Abre **Catalog** en la barra lateral: catálogo `formacion` (o el de tu cuenta), tu esquema y, dentro, el volume `landing`. Lo mismo se puede consultar con SQL.

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOGO}.{ESQUEMA}"))
display(spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Consulta los detalles de tu esquema con `DESCRIBE SCHEMA EXTENDED`.
# MAGIC
# MAGIC **Comprueba:** sale el nombre del esquema, su dueño (tú) y el comentario que puso la configuración.

# COMMAND ----------

display(spark.sql(f"DESCRIBE SCHEMA EXTENDED {CATALOGO}.{___}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Ficheros en el volume
# MAGIC Un volume es una carpeta gobernada por Unity Catalog. Esta celda deja dos ficheros en `landing/lab1`: los parques en CSV y las turbinas en JSON.
# MAGIC
# MAGIC También puedes subir un fichero tuyo: en Catalog, entra en el volume y arrástralo. Después, con el icono de copiar, tienes su ruta.

# COMMAND ----------

RUTA_LAB1 = f"{RUTA_VOL}/lab1"
dbutils.fs.mkdirs(RUTA_LAB1)
df_parques().toPandas().to_csv(f"{RUTA_LAB1}/parques.csv", index=False)
(df_turbinas().withColumn("fecha_puesta_marcha", F.col("fecha_puesta_marcha").cast("string"))
    .toPandas().to_json(f"{RUTA_LAB1}/turbinas.json", orient="records", lines=True))
display(dbutils.fs.ls(RUTA_LAB1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Leer CSV y JSON con Spark
# MAGIC `spark.read` lee ficheros y devuelve un DataFrame. En un CSV hay que decirle que la primera fila son los nombres de columna y, si quieres tipos, que los deduzca.

# COMMAND ----------

parques = (spark.read
           .option("header", True)          # la primera fila son los nombres de columna
           .option("inferSchema", True)     # que deduzca los tipos
           .csv(f"{RUTA_LAB1}/parques.csv"))
display(parques)
parques.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Lee `turbinas.json` con `spark.read.json`, enséñalo y mira su esquema.
# MAGIC
# MAGIC **Comprueba:** salen 240 turbinas con su parque, modelo, potencia y diámetro del rotor. En JSON no hace falta header: los nombres van en cada registro.

# COMMAND ----------

turbinas = spark.read.___(f"{RUTA_LAB1}/turbinas.json")
display(___)
turbinas.___()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · El DataFrame
# MAGIC Un DataFrame es una tabla repartida entre las máquinas del clúster. Lo básico: contar filas, ver columnas y quedarse con algunas.

# COMMAND ----------

print("Parques:", parques.count(), "| Columnas:", parques.columns)
display(parques.select("nombre", "provincia", "operador"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Cuenta cuántas turbinas tiene el parque `P01`.
# MAGIC
# MAGIC **Comprueba:** salen 144: el 60 % de las 240. Ese desequilibrio nos dará juego el martes.

# COMMAND ----------

print(turbinas.filter(F.col("park_id") == ___).___())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · La primera tabla
# MAGIC `saveAsTable` guarda el DataFrame como tabla Delta en tu esquema. A partir de ahí se consulta con SQL y aparece en Catalog Explorer.

# COMMAND ----------

parques.write.mode("overwrite").saveAsTable(t("lab1_parques"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT operador, count(*) AS parques FROM lab1_parques GROUP BY operador

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Guarda las turbinas como `lab1_turbinas` y cuenta con SQL cuántas hay de cada modelo.
# MAGIC
# MAGIC **Comprueba:** salen tres modelos que suman 240. Búscala también en Catalog Explorer y mira sus pestañas.

# COMMAND ----------

turbinas.write.mode("overwrite").saveAsTable(t(___))
display(spark.sql(f"SELECT modelo, count(*) AS turbinas FROM {t('lab1_turbinas')} GROUP BY ___"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para ir más allá (avanzado)
# MAGIC 1. Borra `lab1_turbinas` con `DROP TABLE` y recupérala con `UNDROP TABLE`. ¿Por qué se puede?
# MAGIC 2. Lee el CSV de parques sin `inferSchema`. ¿De qué tipo salen `lat` y `lon`?
# MAGIC 3. Sobrescribe `lab1_parques` dos veces y mira su historial con `DESCRIBE HISTORY`.
