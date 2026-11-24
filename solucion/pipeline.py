from pyspark import pipelines as dp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
])


@dp.table()
def vuelos_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(spark.conf.get("lab.source_path"))
    )
