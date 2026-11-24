"""Funciones de la demo del día 5: lógica sacada del notebook a un módulo que se puede probar."""
from pyspark.sql import DataFrame, functions as F


def limpiar_lecturas(df: DataFrame) -> DataFrame:
    """Quita lecturas duplicadas y convierte el -9999 del sensor en nulo."""
    return (df.dropDuplicates(["turbine_id", "ts", "signal"])
              .withColumn("value", F.when(F.col("value") != -9999, F.col("value"))))
