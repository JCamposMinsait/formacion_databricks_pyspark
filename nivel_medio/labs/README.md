# Curso Databricks & PySpark · Labs

Caso de uso común a los 5 días: **telemetría de aerogeneradores** (6 parques, 240 turbinas, 5 señales por minuto en formato EAV).
Cada alumno trabaja en su propio esquema `formacion.eolica_<usuario>` (lo crea `00_setup/00_config` solo).

## Entorno: Databricks Free Edition
Cada alumno trabaja en su propia cuenta de Free Edition, creada con un correo personal antes del primer día (ver el LEEME del paquete de alumnos). Así cada uno es administrador de su workspace.

Lo que cambia por usar Free Edition:
- Solo hay cómputo serverless. El lab 2b ve el skew sin Spark UI, contando filas por partición y con el Query Profile.
- No se llega a bases de datos de fuera. El JDBC y Federation del día 2 se ven en la teoría, y en el lab los maestros salen del notebook de configuración.
- No hay external locations, grupos de cuenta ni Service Principal. La parte external del lab 1 y la CI/CD del lab 5a se quedan en la teoría, y los filtros del lab 5b usan una tabla de permisos.
- Solo puede haber un pipeline activo. Antes del bundle del día 5 hay que parar el del lab 4c.
- Hay una cuota diaria de cómputo. Los labs generan dos días de telemetría para no agotarla.
- Si no se puede crear el catálogo `formacion`, `00_config` usa el que trae la cuenta.

## Estructura

```
labs/
├─ 00_setup/00_config.py        ← configuración común (la usan todos los notebooks con %run)
├─ 00_setup/reiniciar_lab.py    ← borra lo que crea el lab de un día, para repetirlo desde cero
├─ 00_setup/ponerse_al_dia.py   ← crea las tablas de los días anteriores a quien se haya perdido uno
├─ alumno/                 ← guías para los alumnos: pasos, slide de referencia, pistas y huecos (___ / # TODO)
└─ soluciones/             ← los mismos labs resueltos
```

- Los alumnos trabajan en `alumno/`. Cada paso indica la slide del curso donde está explicado (por ejemplo, "Día 1 · Time travel: cada commit es una versión").
- `soluciones/` tiene además resueltos los retos de todos los labs. En `alumno/` solo están los enunciados.
- Conviene compartir `soluciones/` al terminar cada día, no antes.

## Orden
| Día | Notebook (en alumno/ y soluciones/) | Duración |
|---|---|---|
| 1 | `dia1/lab1a_pyspark_basico` → `lab1b_bronze_delta` | 2 h |
| 2 | `dia2/lab2a_pyspark_silver` → `lab2b_data_skew` | 2 h |
| 3 | `dia3/lab3_medallion_sql_udfs` | 2 h |
| 4 | `dia4/lab4a_simulador_iot` (en paralelo) → `lab4b_autoloader_streaming` → `lab4c_pipeline_declarativo` → `lab4d_jobs` | 2,5 h |
| 5 | `dia5/lab5a_despliegue_bundle.md` → `lab5b_gobernanza_consumo` | 2 h |

Dependencias: el día 2 necesita Bronze (día 1), el día 3 necesita Silver (día 2) y el día 5 necesita Gold (día 3).
Si alguien se pierde un día o rompe sus tablas, `00_setup/ponerse_al_dia` le crea lo que le falta para empezar el día que toque. Para repetir un lab desde cero, `00_setup/reiniciar_lab` borra lo que crea ese día.

## Cómputo
Todo en **serverless**. En el lab 4b, el streaming continuo con `processingTime` necesitaría un clúster clásico, pero el lab funciona entero con `availableNow`.

El pipeline del día 4 escribe en `eolica_<alumno>_pipeline` y el bundle del día 5 en `eolica_<alumno>_bundle`. Así no chocan con las tablas de los labs, que tienen los mismos nombres.

## Importar
*Workspace → Create → Git folder* apuntando al repo, o *Import* del zip. Los `.py` con cabecera `# Databricks notebook source` se abren como notebooks.
