# Reto 02: micro-batches con Data Vault. Hub = primera carga por llave (SCD1); Satelite = historia (SCD2).
import re

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


# ---------- Configuracion (Settings > Configuration) ----------
# Obligatorio: pipelines.suffix (tres letras). Opcionales, con estos valores por defecto:
# raw_catalog=raw_data, raw_schema=bronze, silver_catalog=data_vault, silver_schema=silver,
# gold_catalog=ods, gold_schema=gold, landing_volume=landing_retos_sdp
SUFFIX = spark.conf.get("pipelines.suffix", "").strip().lower()
if not re.fullmatch(r"[a-z]{3}", SUFFIX):
    raise ValueError("Configura pipelines.suffix con tres letras unicas, por ejemplo dsh.")


def config(clave, defecto):
    valor = spark.conf.get(clave, defecto).strip()
    if not re.fullmatch(r"[A-Za-z0-9_]+", valor):
        raise ValueError(f"Valor invalido para {clave}: '{valor}'. Usa solo letras, numeros y guion bajo.")
    return valor


RAW_CATALOG = config("raw_catalog", "raw_data")
RAW_SCHEMA = config("raw_schema", "bronze")
PLATA = f"{config('silver_catalog', 'data_vault')}.{config('silver_schema', 'silver')}"
ORO = f"{config('gold_catalog', 'ods')}.{config('gold_schema', 'gold')}"
VOLUMEN = config("landing_volume", "landing_retos_sdp")

RUTA_ENTRADA = f"/Volumes/{RAW_CATALOG}/{RAW_SCHEMA}/{VOLUMEN}/{SUFFIX}/reto02/entrada"

BRONCE = f"{RAW_CATALOG}.{RAW_SCHEMA}.r02_vuelos_raw_{SUFFIX}"
STAGING = f"r02_stg_vuelos_{SUFFIX}"
HUB_VUELO = f"{PLATA}.r02_hub_vuelo_{SUFFIX}"
SAT_VUELO = f"{PLATA}.r02_sat_vuelo_{SUFFIX}"
POR_AERONAVE = f"{ORO}.r02_vuelos_por_aeronave_{SUFFIX}"

SCHEMA = StructType([
    StructField("evento_id", StringType()),
    StructField("aeronave_id", StringType()),
    StructField("pais_origen", StringType()),
    StructField("velocidad_kmh", DoubleType()),
    StructField("persona_id", StringType()),
    StructField("persona_alias", StringType()),
])

ATRIBUTOS = ["aeronave_id", "pais_origen", "velocidad_kmh", "persona_id", "persona_alias"]


def normalizar(columna):
    """Texto en mayusculas, sin espacios y sin nulos: base estable para calcular hashes."""
    return F.upper(F.trim(F.coalesce(F.col(columna).cast("string"), F.lit(""))))


# ---------- Bronce: el dato tal como llega, mas columnas tecnicas ----------
@dp.table(name=BRONCE, comment="Bronce: eventos JSON tal como llegan, con archivo de origen y fecha de ingesta.")
def r02_vuelos_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(SCHEMA)
        .load(RUTA_ENTRADA)
        .select(
            "*",
            F.col("_metadata.file_path").alias("archivo_origen"),
            F.col("_metadata.file_modification_time").alias("fecha_archivo"),
            F.current_timestamp().alias("fecha_ingesta"),
        )
    )


# ---------- Staging privado: llaves hash y metadatos Data Vault ----------
# fecha_carga = momento en que el archivo llego a la landing; ordena las versiones.
@dp.temporary_view(name=STAGING, comment="Staging: hash key, hashdiff, fecha_carga y fuente_registro.")
def r02_stg_vuelos():
    return spark.readStream.table(BRONCE).select(
        F.sha2(normalizar("evento_id"), 256).alias("hk_vuelo"),
        "evento_id",
        F.sha2(F.concat_ws("||", *[normalizar(c) for c in ATRIBUTOS]), 256).alias("hashdiff"),
        *ATRIBUTOS,
        F.col("fecha_archivo").alias("fecha_carga"),
        F.col("archivo_origen").alias("fuente_registro"),
    )


# ---------- Plata: Hub (SCD1 que conserva la PRIMERA carga de cada llave) ----------
# AUTO CDC se queda con la fila de mayor secuencia. Al usar la fecha en negativo,
# la carga mas antigua "gana" y las llegadas posteriores de la misma llave se ignoran.
dp.create_streaming_table(name=HUB_VUELO, comment="Hub: una fila por evento_id con su primera carga.")
dp.create_auto_cdc_flow(
    target=HUB_VUELO,
    source=STAGING,
    keys=["hk_vuelo"],
    sequence_by=F.expr("-unix_micros(fecha_carga)"),
    column_list=["hk_vuelo", "evento_id", "fecha_carga", "fuente_registro"],
    stored_as_scd_type=1,
)

# ---------- Plata: Satelite (SCD2, nueva version solo si cambia el hashdiff) ----------
dp.create_streaming_table(name=SAT_VUELO, comment="Satelite: historia de atributos; __END_AT nulo = version vigente.")
dp.create_auto_cdc_flow(
    target=SAT_VUELO,
    source=STAGING,
    keys=["hk_vuelo"],
    sequence_by="fecha_carga",
    column_list=["hk_vuelo", "hashdiff", *ATRIBUTOS, "fecha_carga", "fuente_registro"],
    stored_as_scd_type=2,
    track_history_column_list=["hashdiff"],
)


# ---------- Oro: vuelos por aeronave sobre las versiones vigentes ----------
@dp.materialized_view(name=POR_AERONAVE, comment="Oro: eventos y velocidad promedio por aeronave (versiones vigentes).")
def r02_vuelos_por_aeronave():
    return (
        spark.read.table(SAT_VUELO)
        .where("__END_AT IS NULL")
        .groupBy("aeronave_id")
        .agg(
            F.count("*").alias("eventos"),
            F.avg("velocidad_kmh").alias("velocidad_promedio"),
        )
    )
