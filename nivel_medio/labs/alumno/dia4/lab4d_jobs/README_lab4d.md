# Lab 4d · Orquestar con Lakeflow Jobs (~60 min)

Montamos desde la UI un job de 5 tareas con parámetros, paso de valores entre tareas, una condición, reintentos, notificaciones y un fallo provocado para practicar el **Repair run**.

Requisito: tener `silver_telemetria` del lab 2.

## 1 · Crear el job
1. *Jobs & Pipelines* → **Create → Job**. Nombre: `alertas-temperatura-<alumno>`.
2. **Parámetros del job** (panel derecho → *Job parameters*):
   - `zona` = `SUR`
   - `umbral_temp` = `78` (con los datos del curso salen las 2 turbinas con la multiplicadora caliente)
   - `modo_estricto` = `false`

   Los parámetros del job llegan a **todas** las tareas como widgets con el mismo nombre.

## 2 · Tareas
| Task key | Tipo | Qué | Depende de |
|---|---|---|---|
| `configurar` | Notebook | `01_configurar` | ninguna |
| `procesar` | Notebook | `02_procesar` · Retries: 2, cada 1 min · Timeout: 20 min | `configurar` |
| `hay_alertas` | If/else condition | `{{tasks.procesar.values.n_alertas}}` **>** `0` | `procesar` |
| `alertar` | Notebook | `03_alertar` | `hay_alertas` (true) |
| `todo_ok` | Notebook | `04_todo_ok` | `hay_alertas` (false) |

Cómputo: **Serverless** en todas.

## 3 · Notificaciones y disparador
- *Job notifications* → On failure, con vuestro email.
- *Schedule & Triggers*: probad a crear un **Scheduled** (cron cada día a las 07:00) y dejadlo en **Paused**. Mirad también las opciones **File arrival** y **Table update**.

## 4 · Ejecutar
1. **Run now**. En la ejecución mirad el grafo, la duración de cada tarea, la rama que ha tomado la condición y, en cada tarea, la pestaña de *task values*.
2. **Run now with different parameters** con `zona = NORTE` y `umbral_temp = 90`. Debería ir por `todo_ok`.
3. Ahora con `zona = TODAS` y `modo_estricto = true`. La tarea `alertar` falla a propósito y os llega el email.
4. Cambiad el parámetro `modo_estricto` a `false` en la tarea y pulsad **Repair run**: solo se relanza lo fallado, no las tareas que fueron bien.

## 5 · Mirar el histórico con tablas de sistema (si tenéis permiso)
```sql
SELECT job_id, run_id, result_state, period_start_time, period_end_time
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_date() - 1
ORDER BY period_start_time DESC;
```

## 6 · Ver el job como código
Menú `⋮` del job → **View as code** (YAML). Ese YAML es casi lo mismo que pondremos el viernes en el bundle.

## Retos
1. Añadid una tarea de tipo **Pipeline** que lance el pipeline del lab 4c antes de `procesar`.
2. Convertid `procesar` en un **For each** que itere sobre la lista de parques (`{{tasks.configurar.values.parques}}`) y procese cada uno en paralelo.
3. (Examen) ¿Qué diferencia hay entre los parámetros de job y los de tarea? ¿Qué pasa si los dos definen el mismo nombre?
