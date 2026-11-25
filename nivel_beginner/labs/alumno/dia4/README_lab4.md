# Lab 4c · El pipeline declarativo (~45 min)

Es el mismo flujo del lab 4b, pero montado como pipeline. Tiene una Bronze con Auto Loader, una Silver con reglas de calidad y una Gold por turbina y minuto. El código está en `lab4c_pipeline.py` y no hay que tocarlo. El pipeline se crea y se ejecuta desde la interfaz.

Necesitas el simulador en marcha y la tabla `silver_dim_turbina` del lab 3. Si no la tienes, ejecuta `00_setup/ponerse_al_dia` eligiendo el día 4.

## 1 · Crear el pipeline
1. **Jobs & Pipelines → Create → ETL pipeline**. Nombre: `telemetria-<tu usuario>`.
2. En el código fuente, elige el fichero `lab4c_pipeline.py` de esta carpeta.
3. Destino: tu catálogo y el esquema que termina en **`_pipeline`** (`eolica_<tu usuario>_pipeline`). No uses tu esquema de siempre, porque ya tiene tablas con los mismos nombres.
4. En **Configuration**, añade dos claves:
   - `ruta_landing` = `/Volumes/<catálogo>/eolica_<tu usuario>/landing/telemetria_stream`
   - `tabla_maestro` = `<catálogo>.eolica_<tu usuario>.silver_dim_turbina`
5. Cómputo: **Serverless**. Modo: **Triggered**.

## 2 · Ejecutar y mirar
1. **Start**. Se dibuja el grafo con las tres tablas y sus dependencias, que el motor deduce solo.
2. Pincha en `silver_telemetria`. En la pestaña de calidad salen las filas que han cumplido cada regla y las que se han descartado.
3. Vuelve a lanzarlo con el simulador en marcha: solo se procesa lo nuevo.

## 3 · Retos
- Pasa el pipeline a modo **Continuous** un par de minutos y vuelve a Triggered. En la cuenta gratuita el modo continuo gasta cuota.
- Mira qué hace **Full refresh** antes de pulsarlo. Recalcula todo desde cero.

> En la cuenta gratuita solo puede haber un pipeline activo a la vez. Al terminar, déjalo parado.
