# Databricks notebook source
# MAGIC %md
# MAGIC # Reiniciar un lab
# MAGIC
# MAGIC Borra de tu esquema lo que crea el lab del día que elijas, para repetirlo desde cero.
# MAGIC
# MAGIC - **Solo borra lo de ese día.** Si reinicias el día 1, el lab del día 2 se queda sin Bronze hasta que vuelvas a hacer el lab 1 o uses `ponerse_al_dia`.
# MAGIC - No se ejecuta solo desde ningún lab: lo lanzas tú cuando quieras empezar de nuevo.
# MAGIC - Lo creado desde la interfaz (el pipeline y el job del día 4, lo desplegado con el bundle el día 5) se borra desde la interfaz o con la CLI. Al final te dice cómo.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

dbutils.widgets.dropdown("dia", "1", ["1", "2", "3", "4", "5", "todos"], "Día a reiniciar")
dbutils.widgets.dropdown("borrar_ficheros_simulador", "no", ["no", "si"], "Día 4: borrar también los ficheros del simulador")
dbutils.widgets.text("confirmar", "", "Para todos, escribe BORRAR")

DIA = dbutils.widgets.get("dia")
BORRAR_SIMULADOR = dbutils.widgets.get("borrar_ficheros_simulador") == "si"

# Lo que crea cada lab. El orden importa: tablas antes que las funciones de sus filtros y máscaras.
LAB = {
    "1": {"tablas": ["lab1_parques", "lab1_turbinas", "bronze_telemetria"],
          "rutas": [RUTA_RAW_BATCH, f"{RUTA_VOL}/lab1"]},
    "2": {"tablas": []},
    "3": {"tablas": ["gold_kpi_parque_dia", "gold_kpi_turbina_dia", "silver_telemetria_final", "silver_telemetria_wide",
                     "silver_telemetria_enriquecida", "silver_cuarentena", "silver_telemetria", "dim_turbina_scd2",
                     "silver_dim_turbina", "delta_turbinas"],
          "funciones": ["fn_potencia_teorica"]},
    "4": {"tablas": ["reto_gold_minuto", "reto_ultima_lectura", "reto_bronze_rescue", "bronze_telemetria_stream", "job_alertas_temperatura"],
          "rutas": [RUTA_CHECKPOINT, RUTA_SCHEMAS]},
    "5": {"tablas": ["gold_parques", "gold_ubicacion_parques", "permisos_operador"],
          "funciones": ["rf_operador", "cm_coordenada"]},
}

A_MANO = {
    "4": "Borra el pipeline telemetria-<tu_usuario> (Jobs & Pipelines, menú de los tres puntos, Delete): con él se borran sus tablas "
         f"del esquema {ESQUEMA_PIPELINE}. Borra también el job alertas-temperatura-<tu_usuario>.",
    "5": "El repositorio de GitHub y su Git folder se borran a mano. Si desplegaste el bundle: databricks bundle destroy -t dev",
}


def intentar(accion, texto):
    try:
        if callable(accion):
            accion()
        else:
            spark.sql(accion)
        print("  ok   ", texto)
    except Exception as e:
        print("  aviso", texto, "->", str(e).splitlines()[0][:150])


def reiniciar(d):
    cfg = LAB[d]
    print(f"Día {d}")
    for tb in cfg.get("tablas", []):
        intentar(f"DROP TABLE IF EXISTS {t(tb)}", f"tabla {tb}")
    for fn in cfg.get("funciones", []):
        intentar(f"DROP FUNCTION IF EXISTS {t(fn)}", f"función {fn}")
    rutas = list(cfg.get("rutas", []))
    if d == "4" and (BORRAR_SIMULADOR or DIA == "todos"):
        rutas.append(RUTA_RAW_STREAM)
    for r in rutas:
        intentar(lambda r=r: dbutils.fs.rm(r, True), f"ruta {r}")
    if d in A_MANO:
        print("  A MANO:", A_MANO[d])


if DIA == "todos" and dbutils.widgets.get("confirmar") != "BORRAR":
    print("Para borrar lo de todos los días, escribe BORRAR en el cuadro de confirmar y vuelve a ejecutar.")
else:
    for d in (["5", "4", "3", "2", "1"] if DIA == "todos" else [DIA]):
        reiniciar(d)
    print("Hecho.")
