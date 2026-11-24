# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 4 · Simulador IoT en tiempo real (cada 10 s)
# MAGIC
# MAGIC Abridlo en **otra pestaña** y dejadlo corriendo mientras trabajáis en el lab 4b/4c.
# MAGIC Cada 10 s escribe un fichero JSON con una lectura por turbina y señal en `telemetria_stream/`.
# MAGIC
# MAGIC Incluye a propósito:
# MAGIC - **Datos tardíos**: ~3 % de las lecturas llegan con un `ts` de hasta 3 min atrás (para entender los *watermarks*).
# MAGIC - **Anomalía**: la turbina `WTG-0013` se calienta progresivamente (para la alerta de la práctica).
# MAGIC - **Cambio de esquema** a mitad: a partir de la iteración 30 aparece el campo `firmware_build` (Auto Loader *schema evolution*).

# COMMAND ----------

# MAGIC %run ../../00_setup/00_config

# COMMAND ----------

dbutils.widgets.text("minutos", "20")
dbutils.widgets.text("intervalo_s", "10")
MINUTOS, INTERVALO = int(dbutils.widgets.get("minutos")), int(dbutils.widgets.get("intervalo_s"))

import json, random, time, math, os, datetime as dt

dbutils.fs.mkdirs(RUTA_RAW_STREAM)
turbinas = [(r.turbine_id, r.park_id, r.potencia_nominal_kw, r.diametro_rotor_m) for r in df_turbinas().collect()]
iteraciones = MINUTOS * 60 // INTERVALO

for it in range(iteraciones):
    ahora = dt.datetime.now(dt.timezone.utc)
    lineas = []
    for tid, park, nominal, rotor in turbinas:
        ts = ahora - dt.timedelta(seconds=random.randint(30, 180)) if random.random() < 0.03 else ahora
        v = max(0.0, 7 + 3 * math.sin(it / 20) + random.gauss(0, 1.5) + (2 if park == "P03" else 0))
        p = 0.0 if v < 3 or v >= 25 else min(nominal, 0.5 * 1.225 * math.pi * (rotor / 2) ** 2 * v ** 3 * 0.40 / 1000)
        temp = 45 + p / nominal * 25 + random.gauss(0, 1) + (it * 0.4 if tid == "WTG-0013" else 0)
        senales = {"wind_speed": v, "power_kw": p + random.gauss(0, 30), "rotor_rpm": min(14.5, v * 1.15),
                   "temp_gearbox_c": temp, "vibration_mm_s": abs(random.gauss(0.6, 0.3))}
        for sig, val in senales.items():
            reg = {"turbine_id": tid, "park_id": park, "ts": ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                   "signal": sig, "value": round(val, 3), "quality": "GOOD", "fw_version": "3.2.1"}
            if it >= 30:
                reg["firmware_build"] = "2026.11-r7"
            lineas.append(json.dumps(reg))
    nombre = f"{RUTA_RAW_STREAM}/lote_{ahora:%Y%m%d_%H%M%S}_{it:05d}.json"
    with open(nombre, "w") as f:          # los Volumes se escriben como un fichero local
        f.write("\n".join(lineas))
    print(f"[{it+1}/{iteraciones}] {nombre.split('/')[-1]} · {len(lineas)} lecturas")
    time.sleep(INTERVALO)
