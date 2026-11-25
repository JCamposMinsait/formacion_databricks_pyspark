# Lab 4 · Streaming y pipelines declarativos

Orden recomendado:

1. **lab4a_simulador_iot**: arrancadlo en otra pestaña. Por defecto dura 20 min y genera un lote cada 10 s.
2. **lab4b_autoloader_streaming**: COPY INTO, Auto Loader, checkpoints y ventanas con watermark.
3. **lab4c_pipeline_declarativo** (.py o .sql): el mismo flujo como *Lakeflow Spark Declarative Pipeline*.
4. **lab4d_jobs**: orquestación con Lakeflow Jobs, con parámetros, task values, If/else, reintentos y Repair run (ver `lab4d_jobs/README_lab4d.md`).

## Cómo crear el pipeline del 4c (UI)

1. *Jobs & Pipelines* → **Create → ETL pipeline**.
2. Nombre: `telemetria-<alumno>`. Catálogo `formacion`, esquema **`eolica_<alumno>_pipeline`** (lo crea `00_config`). No uséis vuestro esquema de siempre: ya tiene las tablas `bronze_telemetria` y `silver_telemetria` de los días 1 y 2, y el pipeline no puede crear tablas con nombres que ya existen.
3. Código fuente: añadid `lab4c_pipeline_declarativo.py` (o el `.sql`).
4. En *Configuration*, añadid:
   - `ruta_landing` = `/Volumes/formacion/eolica_<alumno>/landing/telemetria_stream`
   - `tabla_maestro` = `formacion.eolica_<alumno>.silver_dim_turbina` (creada en el lab 2)
5. Cómputo **serverless**, modo **Triggered**. Ejecutad (*Run pipeline*).
6. Mirad el **grafo**, las **métricas de expectations** (cuántas filas ha tirado cada regla) y el *event log*.
7. Pasad a modo **Continuous** con el simulador corriendo y ved cómo se actualiza solo.

## Preguntas para el debate (DLT/LDP vs streaming "a mano")
- ¿Quién gestiona checkpoints, reintentos y orden? ¿Qué pasa si quiero sacarlo de Databricks mañana?
- Desde Spark 4.1 los *Declarative Pipelines* forman parte de Apache Spark (open source), así que el código `pyspark.pipelines` es portable. Las expectations avanzadas, AUTO CDC y el cómputo serverless siguen siendo de Databricks.

> **En Free Edition** solo puede haber un pipeline activo a la vez, y el modo Continuous gasta cuota. Probadlo un par de minutos y paradlo.
