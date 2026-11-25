# Lab 5a · Bundle y despliegue E2E (~50 min)

> **En Free Edition:** antes de desplegar, parad el pipeline del lab 4c, porque solo puede haber uno activo. En `databricks.yml` hay que poner la URL de vuestro workspace. La parte de CI/CD necesita un Service Principal, así que aquí solo se lee.

El job ya lo conocéis del jueves (lab 4d). Aquí lo convertimos en código versionado y desplegado por CI/CD.
Día 5 · Git folders · Declarative Automation Bundles · Anatomía de databricks.yml · Los seis comandos · CI/CD

## Antes de empezar
- Databricks CLI instalada (`winget install Databricks.DatabricksCLI`) y `databricks auth login --host <tu workspace> --profile curso`.
- Acceso al repo Git del curso.

## Ejercicios
1. **Git folder.** Workspace → Create → Git folder con el repo del curso. Crea la rama `feature/<tu_nombre>`.
2. **Valida y despliega en dev.**
   - Pon el `host` de tu workspace en `databricks.yml`.
   - Ejecuta `databricks bundle validate -t dev -p curso`. En la salida, ¿qué nombre tendrá el job? ¿Por qué lleva un prefijo?
   - Despliega con `databricks bundle deploy -t dev -p curso` y busca el job en Jobs & Pipelines.
3. **Completa el job.** En `resources/job.yml` falta la parte final (está marcada con TODO): una tarea `hay_alertas` de tipo `condition_task` y una tarea `notificar_mantenimiento` que solo se ejecute si la condición es verdadera. Vuelve a validar y desplegar.
4. **Ejecuta.** El pipeline lee los ficheros que dejó el simulador del lab 4a (si no queda ninguno, arráncalo unos minutos) y escribe en el esquema `eolica_<tu_usuario>_bundle`. Lanza `databricks bundle run telemetria_job -t dev -p curso`. En la ejecución, mira el grafo, la condición y el task value `alertas`.
5. **Rómpelo y repáralo.** Cambia en `resources/pipeline.yml` la `ruta_landing` por una ruta que no exista, despliega, ejecuta, mira el error, arréglalo y usa **Repair run**.
6. **Pull Request.** Commit, push y PR. Mira en el workflow cómo la CI ejecutaría `bundle validate` y el despliegue a producción con un Service Principal. En Free Edition no lo hay, así que esta parte se lee pero no se ejecuta.

## Preguntas de examen
- ¿Qué cambia entre `mode: development` y `mode: production`?
- ¿Dónde se sobrescribe una variable para un entorno concreto?
- ¿Qué comando valida sin desplegar? ¿Y cuál borra lo desplegado?

Solución: `labs/soluciones/dia5/`.
