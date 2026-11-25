# Labs del curso Databricks y PySpark

Cada lab tiene dos versiones:
- `alumno/`: los labs paso a paso. El código de los pasos difíciles ya viene escrito y explicado. En los ejercicios **Ahora tú** los alumnos completan los `___`. Al final de cada lab hay unos retos opcionales.
- `soluciones/`: lo mismo, con los ejercicios y los retos resueltos. No se reparte a los alumnos.

## Orden
| Día | Lab | Duración |
|---|---|---|
| 1 | `dia1/lab1a_plataforma_y_ficheros` → `lab1b_telemetria_bronze` | 2 h |
| 2 | `dia2/lab2a_pyspark_paso_a_paso` → `lab2b_rendimiento_basico` | 2 h |
| 3 | `dia3/lab3_delta_medallion` | 2 h |
| 4 | `dia4/lab4a_simulador` → `lab4b_auto_loader` → `lab4c_pipeline` (guía `README_lab4.md`) → `lab4d_job` (guía `README_lab4d.md`) | 2 h |
| 5 | `dia5/lab5a_proyecto_ci.md` (con la carpeta `proyecto_eolica`) → `lab5b_gobierno_consumo` | 2 h |

## Lo que deja cada día
- **Día 1:** `bronze_telemetria`, la tabla de la que parte todo.
- **Día 2:** nada obligatorio. Es práctica sobre la Bronze.
- **Día 3:** el maestro `silver_dim_turbina`, `silver_telemetria`, `silver_cuarentena` y las Gold `gold_kpi_turbina_dia` y `gold_kpi_parque_dia`.
- **Día 4:** `bronze_telemetria_stream`, el pipeline (en el esquema `eolica_<usuario>_pipeline`) y el job.
- **Día 5:** permisos, un dashboard y un Genie space sobre la Gold, y el proyecto en GitHub con su CI.

Si alguien se pierde un día, `00_setup/ponerse_al_dia` le crea lo que le falte eligiendo el día en el que empieza.

## Entorno: Databricks Free Edition
Cada alumno trabaja en su propia cuenta, creada con un correo personal antes del primer día (ver el LEEME del paquete de alumnos).
- Solo hay cómputo serverless, así que no hay Spark UI y el streaming se lanza con `availableNow`.
- Hay una cuota diaria de cómputo. Por eso los labs generan solo dos días de telemetría.
- Solo puede haber un pipeline activo a la vez.
- No hay grupos de cuenta ni Service Principal, así que los permisos se dan a `account users`.
- Para el día 5 hace falta una cuenta de GitHub.
- Si no se puede crear el catálogo `formacion`, `00_config` usa el que trae la cuenta.

## Notebooks de apoyo (`00_setup`)
- `00_config`: catálogo, esquema personal, volume y funciones comunes. Lo ejecuta la primera celda de cada lab.
- `ponerse_al_dia`: crea lo que falte para empezar un día.
- `reiniciar_lab`: borra lo creado en un día para repetirlo.
