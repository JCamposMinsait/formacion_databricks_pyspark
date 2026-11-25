# telemetria_bundle · Declarative Automation Bundle

```
telemetria_bundle/
├── databricks.yml              ← bundle, variables y targets (dev / prod)
├── resources/
│   ├── pipeline.yml            ← Lakeflow Spark Declarative Pipeline
│   └── job.yml                 ← Lakeflow Job: maestros → pipeline → calidad → (si alertas) notificar
├── src/
│   ├── pipeline/telemetria_pipeline.py
│   └── notebooks/01_preparar_maestros.py · 02_control_calidad.py · 03_notificar.py
├── .github/workflows/deploy.yml   ← CI/CD con GitHub Actions
```

## Paso a paso (Windows / PowerShell)

```powershell
# 1. CLI (una vez)
winget install Databricks.DatabricksCLI
databricks -v

# 2. Login (OAuth U2M, abre el navegador)
databricks auth login --host https://dbc-XXXXXXXX-XXXX.cloud.databricks.com --profile curso

# 3. Editar el host en databricks.yml y validar
cd telemetria_bundle
databricks bundle validate -t dev -p curso

# 4. Desplegar mi copia de dev y ejecutar
databricks bundle deploy -t dev -p curso
databricks bundle run telemetria_job -t dev -p curso

# 5. Ver qué ha creado
databricks bundle summary -t dev -p curso
```

En dev, el pipeline lee del mismo landing en el que escribe el simulador del lab 4a (`/Volumes/formacion/eolica_<usuario>/landing/telemetria_stream`), así que si quedan ficheros del jueves no hay que copiar nada. Si no queda ninguno, arrancad el simulador unos minutos.

Las tablas se crean en un esquema aparte, `eolica_<usuario>_bundle`, para no chocar con las de los labs. Si la ruta del landing no coincide con la vuestra (pasa con algunos nombres de usuario), cambiad `ruta_landing` en el target `dev`.

## Producción
- El target `prod` lleva `run_as` con un **Service Principal**: los jobs se ejecutan con su identidad, no con la de quien despliega.
- Solo la CI despliega a prod (`.github/workflows/deploy.yml`). Las personas, como mucho, ejecutan `validate`.
- Para destruir lo desplegado en dev: `databricks bundle destroy -t dev`.

## En Free Edition
- Si `databricks auth login` falla, genera un token en Settings → Developer → Access tokens y configura la CLI con `databricks configure --host <url de tu workspace> --profile curso`.
- Antes de desplegar, parad o borrad el pipeline del lab 4c, porque en Free Edition solo puede haber un pipeline activo.
- En `databricks.yml`, poned en `host` la URL de vuestro workspace y, si vuestro catálogo no es `formacion`, cambiadlo en `catalogo` y en `ruta_landing`.
- El target `prod` y el workflow de GitHub necesitan un Service Principal, que en Free Edition no existe. Se leen y se explican, pero no se ejecutan.
