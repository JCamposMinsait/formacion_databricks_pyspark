# Databricks notebook source
# MAGIC %run ../labs/00_setup/00_config

# COMMAND ----------

# Borra lo que crean las demos (objetos y rutas que empiezan por demo_ o _demo_). No toca las tablas de los labs.
# Cada demo lo llama al principio con su día: %run ./00_limpieza_demos $dia="1"
# Ejecutado directamente con dia = todos deja el esquema limpio al acabar.

dbutils.widgets.dropdown("dia", "todos", ["1", "2", "3", "4", "5", "todos"], "Día a limpiar")
DIA = dbutils.widgets.get("dia")

# Lo que crea cada notebook de demos. El orden importa: vistas antes que tablas,
# hechos antes que dimensiones (por la FK) y tablas antes que las funciones de sus row filters.
LIMPIEZA = {
    "1": {
        "tablas": ["demo_formatos_delta", "demo_fmt", "demo_log", "demo_escritura", "demo_tt",
                   "demo_small", "demo_sin_cluster", "demo_liquid", "demo_telemetria_grande"],
        "patrones": ["demo_drop_*"],
        "rutas": ["hola.txt", "formatos"],
    },
    "2": {
        "rutas": ["demo_dia2"],
        "scopes": ["demo-secretos"],
    },
    "3": {
        "vistas_materializadas": ["demo_mv_energia_parque"],
        "vistas": ["demo_v_energia_parque"],
        "tablas": ["demo_hechos_energia", "demo_dim_turbina", "demo_merge", "demo_scd2_backup", "demo_scd2"],
        "funciones": ["demo_fn_mwh"],
    },
    "4": {
        "tablas": ["demo_stream_gold", "demo_ultima_lectura", "demo_stream_bronze"],
        "rutas": ["demo_stream_in", "_demo_ck", "_demo_schema"],
    },
    "5": {
        "tablas": ["demo_parques", "demo_permisos"],
        "funciones": ["demo_rf", "demo_cm"],
    },
}

# demo_telemetria la regeneran los días 1, 2 y 3 en su preparación. Solo se borra con "todos".
COMUNES = {"tablas": ["demo_telemetria"]}


def intentar(accion, texto):
    try:
        if callable(accion):
            accion()
        else:
            spark.sql(accion)
        print("  ok   ", texto)
    except Exception as e:
        print("  aviso", texto, "->", str(e).splitlines()[0][:150])


def limpiar(cfg):
    for v in cfg.get("vistas_materializadas", []):
        intentar(f"DROP MATERIALIZED VIEW IF EXISTS {t(v)}", f"vista materializada {v}")
    for v in cfg.get("vistas", []):
        intentar(f"DROP VIEW IF EXISTS {t(v)}", f"vista {v}")
    for patron in cfg.get("patrones", []):
        for fila in spark.sql(f"SHOW TABLES IN {CATALOGO}.{ESQUEMA} LIKE '{patron}'").collect():
            intentar(f"DROP TABLE IF EXISTS {t(fila.tableName)}", f"tabla {fila.tableName}")
    for tb in cfg.get("tablas", []):
        intentar(f"DROP TABLE IF EXISTS {t(tb)}", f"tabla {tb}")
    for fn in cfg.get("funciones", []):
        intentar(f"DROP FUNCTION IF EXISTS {t(fn)}", f"función {fn}")
    for sc in cfg.get("scopes", []):
        def borrar_scope(sc=sc):
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            if sc in [s.name for s in w.secrets.list_scopes()]:
                w.secrets.delete_scope(scope=sc)
        intentar(borrar_scope, f"scope {sc}")
    for r in cfg.get("rutas", []):
        intentar(lambda r=r: dbutils.fs.rm(f"{RUTA_VOL}/{r}", True), f"ruta {r}")


dias = ["5", "4", "3", "2", "1"] if DIA == "todos" else [DIA]
for d in dias:
    print(f"Día {d}")
    limpiar(LIMPIEZA[d])
if DIA == "todos":
    print("Comunes")
    limpiar(COMUNES)
print(f"Limpieza terminada ({DIA})")
