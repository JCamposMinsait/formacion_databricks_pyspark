# Lab 5a · Módulo, tests, Git y CI/CD (~50 min)

Vamos a sacar la lógica de limpieza a un módulo, escribirle un test, subirlo a GitHub y ver cómo la CI pasa los tests sola en cada cambio. Si da tiempo, también se puede activar la CD para que cada merge despliegue el proyecto en tu Databricks.

Necesitas una cuenta de GitHub. Todo el setup está en el `README.md` de `proyecto_eolica`.

## 1 · Preparar el repositorio
Sigue los apartados 1 y 2 del README para crear tu repo `proyecto-eolica` en GitHub y su Git folder en Databricks. En **Actions** verás que la CI ha arrancado y falla.

## 2 · Completar el módulo y el test
Crea la rama `mi-cambio` (apartado 3 del README) y en ella:
1. Abre `src/eolica/limpieza.py` y completa la función `a_mw`. Tiene que devolver el DataFrame con una columna nueva, `potencia_mw`, que es la columna indicada dividida entre 1000.
2. Abre `tests/test_limpieza.py` y completa `test_a_mw`. Una lectura de 4500 kW tiene que dar 4.5 MW.
3. Abre `notebooks/usar_modulo.py` y ejecútalo. Aplica tus funciones a tu Bronze.

## 3 · Commit, Pull Request y CI
1. **Commit & Push** desde el Git folder.
2. En GitHub, abre la Pull Request de `mi-cambio` a `main` y mira cómo la CI pasa los tests.
3. Cuando esté en verde, haz el merge.

## 4 · Retos
- Rompe a propósito un test (por ejemplo, espera 5 en vez de 4.5), haz commit y mira cómo la CI se pone en rojo en la PR. Después arréglalo.
- Activa la CD (apartado 4 del README), haz un merge a `main` y ejecuta en Databricks el job que se despliega.
- Añade una función `quitar_negativos` que ponga a cero las potencias negativas, con su test.
