# Lab 4d · El job (~40 min)

Montamos desde la UI un job de 5 tareas con parámetros, paso de valores entre tareas, una condición, reintentos, notificaciones y un fallo provocado para practicar el **Repair run**.

Necesitas la tabla `silver_telemetria` del lab 3. Si no la tienes, ejecuta `00_setup/ponerse_al_dia` eligiendo el día 4.

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
| `configurar` | Notebook | `01_configurar` | - |
| `procesar` | Notebook | `02_procesar` · Retries: 2, cada 1 min · Timeout: 20 min | `configurar` |
| `hay_alertas` | If/else condition | `{{tasks.procesar.values.n_alertas}}` **>** `0` | `procesar` |
| `alertar` | Notebook | `03_alertar` | `hay_alertas` (true) |
| `todo_ok` | Notebook | `04_todo_ok` | `hay_alertas` (false) |

Cómputo: **Serverless** en todas.

## 3 · Notificaciones y disparador
- En *Job notifications*, añade tu email en On failure.
- En *Schedule & Triggers*, crea un **Scheduled** (cron cada día a las 07:00) y déjalo en **Paused**. Mira también las opciones **File arrival** y **Table update**.

## 4 · Ejecutar
1. **Run now**. En la ejecución puedes ver el grafo, la duración de cada tarea, la rama que ha tomado la condición y, dentro de cada tarea, la pestaña de *task values*.
2. **Run now with different parameters** con `zona = NORTE` y `umbral_temp = 90`. Debería ir por `todo_ok`.
3. Ahora con `zona = TODAS` y `modo_estricto = true`. La tarea `alertar` falla a propósito y te llega el email.
4. Cambia el parámetro `modo_estricto` a `false` en la tarea y pulsa **Repair run**. Solo se relanza lo que ha fallado.

## 5 · Mirar el histórico con tablas de sistema (si tienes permiso)
```sql
SELECT job_id, run_id, result_state, period_start_time, period_end_time
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_date() - 1
ORDER BY period_start_time DESC;
```

## 6 · Ver el job como código
Menú `⋮` del job → **View as code** (YAML). Ese YAML es casi lo mismo que pondremos el viernes en el bundle.

## Retos
Van de menos a más. El último es para quien vaya sobrado.

1. (Examen) ¿Qué diferencia hay entre los parámetros de job y los de tarea? ¿Qué pasa si los dos definen el mismo nombre?
2. Añade al final una tarea `resumen` con un notebook nuevo que escriba cuántas alertas ha habido, leyendo el task value `n_alertas` de `procesar`. Tiene que ejecutarse por las dos ramas de la condición. Prueba qué opción de **Run if dependencies** hace falta.
3. Haz que `resumen` guarde además una fila por ejecución en la tabla `job_historico`, con la zona, el número de alertas y la hora. Lanza el job tres veces con zonas distintas y consulta la tabla.
4. Añade una tarea de tipo **Pipeline** que lance el pipeline del lab 4c antes de `procesar`.
5. Convierte `procesar` en un **For each** que itere sobre la lista de parques (`{{tasks.configurar.values.parques}}`) y procese cada uno en paralelo.
