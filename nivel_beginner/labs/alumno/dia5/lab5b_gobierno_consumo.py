# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 5b · Gobierno y consumo
# MAGIC
# MAGIC Duración: 1 h aproximadamente.
# MAGIC
# MAGIC Vamos a dar permisos sobre la Gold, ver de dónde sale cada dato y dejarla lista para negocio con un dashboard y un Genie space.
# MAGIC
# MAGIC > Necesitas las tablas Gold del lab 3. Si no las tienes, ejecuta `00_setup/ponerse_al_dia` eligiendo el día 5.
# MAGIC
# MAGIC En las celdas de **Ahora tú** hay que completar lo que está marcado con `___`. En la última escribes tú el código entero. Los retos del final son opcionales y van de menos a más.

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

GRUPO = "account users"     # en la cuenta gratuita no hay grupos propios: este incluye a todos los usuarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Permisos
# MAGIC Para leer una tabla hacen falta tres permisos: USE CATALOG, USE SCHEMA y SELECT. Se dan a grupos, nunca a personas sueltas.

# COMMAND ----------

for sentencia in [f"GRANT USE CATALOG ON CATALOG {CATALOGO} TO `{GRUPO}`",
                  f"GRANT USE SCHEMA ON SCHEMA {CATALOGO}.{ESQUEMA} TO `{GRUPO}`",
                  f"GRANT SELECT ON TABLE {t('gold_kpi_parque_dia')} TO `{GRUPO}`"]:
    spark.sql(sentencia)
display(spark.sql(f"SHOW GRANTS ON TABLE {t('gold_kpi_parque_dia')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Quita el SELECT que acabas de dar y vuelve a mirar los permisos de la tabla.
# MAGIC
# MAGIC El SELECT de account users ya no debería aparecer.

# COMMAND ----------

spark.sql(f"___ SELECT ON TABLE {t('gold_kpi_parque_dia')} FROM `{GRUPO}`")
display(spark.sql(f"SHOW GRANTS ON TABLE {t('gold_kpi_parque_dia')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Linaje
# MAGIC Abre `gold_kpi_parque_dia` en Catalog Explorer y entra en la pestaña **Lineage**. Ahí se ve de qué tablas sale, hasta la Bronze, sin haber configurado nada. Lo mismo se puede consultar en las tablas de sistema si tu cuenta las tiene.

# COMMAND ----------

try:
    display(spark.sql(f"""
        SELECT source_table_full_name, target_table_full_name, max(event_time) AS ultima_vez
        FROM system.access.table_lineage
        WHERE target_table_full_name LIKE '{CATALOGO}.{ESQUEMA}.gold%'
        GROUP BY ALL ORDER BY target_table_full_name"""))
except Exception as e:
    print("Las tablas de sistema no están disponibles en esta cuenta. Usa la pestaña Lineage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Un dashboard
# MAGIC Las consultas de un dashboard se llaman datasets. Esta celda escribe dos, con el nombre completo de tus tablas, listas para pegar.
# MAGIC
# MAGIC 1. **New → Dashboard → Data → Create from SQL** y pega cada consulta.
# MAGIC 2. En **Canvas**: una gráfica de barras con la energía por parque y una tabla con las turbinas a revisar.
# MAGIC 3. Añade un filtro por fecha y otro por parque.
# MAGIC 4. **Publish**.

# COMMAND ----------

DATASETS = {
    "Energía por parque y día": f"SELECT fecha, parque, energia_mwh FROM {t('gold_kpi_parque_dia')}",
    "Turbinas a revisar": f"SELECT fecha, turbine_id, parque, rendimiento_medio, temp_gearbox_max FROM {t('gold_kpi_turbina_dia')} WHERE rendimiento_medio < 0.8",
}
for nombre, consulta in DATASETS.items():
    print(f"{nombre}\n{consulta}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Escribe un tercer dataset con la energía total de cada día, sumando todos los parques, y pruébalo aquí antes de llevarlo al dashboard.
# MAGIC
# MAGIC Deberían salir dos filas, una por día, con la energía total de todos los parques.

# COMMAND ----------

consulta = f"SELECT fecha, round(sum(___), 2) AS energia_mwh FROM {t('gold_kpi_parque_dia')} GROUP BY ___ ORDER BY fecha"
display(spark.sql(consulta))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Genie
# MAGIC Un Genie space contesta preguntas en lenguaje natural sobre tus tablas. Acierta más con pocas tablas, bien comentadas y con instrucciones.
# MAGIC
# MAGIC 1. **Genie → New**. Añade las tablas que escribe la celda siguiente.
# MAGIC 2. En **Instructions**, pega las instrucciones.
# MAGIC 3. Pregunta, por ejemplo: *¿Qué parque produjo más energía el último día?* y mira el SQL que genera.

# COMMAND ----------

print("TABLAS")
for x in ["gold_kpi_parque_dia", "gold_kpi_turbina_dia", "silver_dim_turbina"]:
    print("  ", t(x))
print("""
INSTRUCCIONES
- Responde siempre en español.
- Producción o generación significa energia_mwh.
- El factor de capacidad se muestra en porcentaje, con un decimal.
- Una turbina a revisar es la que tiene rendimiento_medio por debajo de 0,8.
- Nombra los parques por su nombre (columna parque), no por el código.
- Si no se indica fecha, usa el último día disponible.""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ahora tú
# MAGIC Escribe la consulta SQL que debería generar Genie para la pregunta *¿Cuántas turbinas a revisar hay en cada parque el último día?* y compárala con la que genera. Esta vez escribes tú la consulta entera.
# MAGIC
# MAGIC Deberían salir los parques con turbinas a revisar, con al menos una en los parques de las turbinas degradadas (WTG-0042, WTG-0117 y WTG-0199).

# COMMAND ----------

# Sobre gold_kpi_turbina_dia, cuenta por parque las turbinas con rendimiento_medio por debajo de 0,8 en el último día


# COMMAND ----------

# MAGIC %md
# MAGIC ## Retos
# MAGIC Van de menos a más. Del 3 al 5 van seguidos.
# MAGIC
# MAGIC 1. Da a `account users` lectura sobre todo tu esquema de una vez, con `GRANT SELECT ON SCHEMA`. Mira con `SHOW GRANTS` el esquema y una de sus tablas. ¿De dónde le viene el permiso a la tabla? Después quítalo.
# MAGIC 2. Añade al dashboard un contador con la energía total del último día y un gráfico de líneas con el factor de capacidad medio de cada parque por día.
# MAGIC 3. Un filtro de filas. Crea una tabla de permisos `permisos_operador` (usuario, operador) y una función que solo deje ver los parques de tus operadores. Aplícala a una copia de `gold_kpi_parque_dia` con el operador de cada parque. La función devuelve verdadero o falso para cada fila y se aplica con `ALTER TABLE ... SET ROW FILTER función ON (columna)`.
# MAGIC 4. Una máscara que redondee las coordenadas de los parques salvo para quien tenga permiso. Se aplica con `ALTER TABLE ... ALTER COLUMN lat SET MASK función`.
# MAGIC 5. Con el filtro puesto, pregúntale a Genie por *Costa da Morte* sobre esa tabla. ¿Por qué no sale nada?
