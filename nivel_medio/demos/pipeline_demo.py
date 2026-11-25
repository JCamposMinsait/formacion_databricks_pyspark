"""
Pipeline de la demo del día 4. Lo crea y lo lanza el notebook demos_dia4, no hace falta tocarlo.

Lee los JSON que escribe la demo en demo_stream_in (clave ruta_demo de la configuración)
y escribe demo_pipeline_bronze y demo_pipeline_silver en el esquema <esquema>_pipeline.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RUTA_DEMO = spark.conf.get("ruta_demo")


@dp.table(name="demo_pipeline_bronze", comment="Lecturas de la demo tal cual llegan")
def demo_pipeline_bronze():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaHints", "ts TIMESTAMP, value DOUBLE")
            .load(RUTA_DEMO)
            .select("turbine_id", "ts", "signal", "value", F.col("_metadata.file_name").alias("fichero")))


@dp.table(name="demo_pipeline_silver", comment="Lecturas de la demo que cumplen las reglas")
@dp.expect_all_or_drop({"valor_presente": "value IS NOT NULL", "sin_centinela": "value <> -9999"})   # descarta la fila
@dp.expect("temperatura_razonable", "value < 120")                                                     # solo avisa
def demo_pipeline_silver():
    return spark.readStream.table("demo_pipeline_bronze")
