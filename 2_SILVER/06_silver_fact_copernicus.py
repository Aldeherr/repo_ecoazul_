# Databricks notebook source
# MAGIC %md
# MAGIC ## 1.  Configuración Inicial

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta


SCOPE = "keyvault-scope" 
STORAGE_ACCOUNT = "dataecoazul"

STORAGE_KEY = dbutils.secrets.get(
    scope=SCOPE,
    key="storage-key"
)

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    STORAGE_KEY
)

BRONZE_BASE = (
    f"wasbs://bronze@{STORAGE_ACCOUNT}.blob.core.windows.net/copernicus/"
)

print("Configuración completada")
print("Bronze base:", BRONZE_BASE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Listado de parquets + filtro incremental

# COMMAND ----------

def lsR_parquet(path: str):
    """Lista recursivamente todos los archivos .parquet en un path y subdirectorios."""
    out = []
    try:
        for fi in dbutils.fs.ls(path):
            if fi.isFile():
                if fi.path.lower().endswith(".parquet"):
                    out.append(fi.path)
            else:
                out.extend(lsR_parquet(fi.path))
    except Exception as e:
        print(f"Error listando {path}: {e}")
    return out

DAYS_LOOKBACK = 2  # pon None si quieres procesar todo el histórico

print(" Listando parquets Bronze (recursivo)...")
all_parquets = lsR_parquet(BRONZE_BASE)

if not all_parquets:
    raise Exception(f"No se encontraron parquets en {BRONZE_BASE}")

print(f" Encontrados {len(all_parquets)} parquets (total)")

# Filtrar por lookback: buscamos YYYYMMDD en el path (copernicus_forecast_48h_YYYYMMDD)
if DAYS_LOOKBACK:
    valid_dates = [
        (datetime.utcnow() - timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS_LOOKBACK + 1)
    ]
    parquet_files = [p for p in all_parquets if any(d in p for d in valid_dates)]
    if not parquet_files:
        print("No hubo match por fechas. Procesando TODO.")
        parquet_files = all_parquets
else:
    parquet_files = all_parquets

print(f" Parquets a procesar: {len(parquet_files)}")
print("Muestra últimos 5:")
for p in sorted(parquet_files)[-5:]:
    print("  ", p)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de Bronze + normalización base

# COMMAND ----------

# Leer Bronze Copernicus
df_bronze = spark.read.parquet(*parquet_files)

print("Lectura Bronze completada")
print("Conteo registros:", df_bronze.count())

print("Schema Bronze:")
df_bronze.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1Normalización mínima de columnas

# COMMAND ----------

expected_columns = [
    "time",
    "depth",
    "latitude",
    "longitude",
    "thetao",
    "so",
    "uo",
    "vo",
    "zona_id",
    "nombre_zona",
    "tipo_dato"
]

for c in expected_columns:
    if c not in df_bronze.columns:
        df_bronze = df_bronze.withColumn(c, F.lit(None))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Proyección base para Silver

# COMMAND ----------

print(df_bronze.columns)
for p in sorted(parquet_files)[-10:]:
    print(p)


# COMMAND ----------

if "timestamp_ingesta" in df_bronze.columns:
    run_ts_expr = F.to_timestamp("timestamp_ingesta")
else:
    run_ts_expr = F.current_timestamp()

df_base = df_bronze.select(
    F.col("zona_id").cast("string").alias("zona_id"),
    F.date_trunc("hour", F.to_timestamp("time")).alias("timestamp_forecast"),
    F.col("latitude").cast("double").alias("lat"),
    F.col("longitude").cast("double").alias("lon"),
    F.col("thetao").cast("double").alias("thetao"),
    F.col("so").cast("double").alias("so"),
    F.col("uo").cast("double").alias("uo"),
    F.col("vo").cast("double").alias("vo"),
    run_ts_expr.alias("run_ts"),
    F.lit("copernicus").alias("source"),
    F.current_timestamp().alias("created_at")
)
print("Base normalizada creada")
print("Registros:", df_base.count())
print("df_base creado. Columnas:", df_base.columns)
display(df_base.limit(5))


# COMMAND ----------

latest = sorted(all_parquets)[-1]
print("Ultimo parquet:", latest)

df_check = spark.read.parquet(latest)
print(df_check.columns)
df_check.select("zona_id", "time").limit(3).show()

# COMMAND ----------

df_bronze.groupBy("tipo_dato").agg(
    F.count("*").alias("rows"),
    F.sum(F.col("thetao").isNotNull().cast("int")).alias("thetao_not_null"),
    F.sum(F.col("so").isNotNull().cast("int")).alias("so_not_null"),
    F.sum(F.col("uo").isNotNull().cast("int")).alias("uo_not_null"),
    F.sum(F.col("vo").isNotNull().cast("int")).alias("vo_not_null"),
).orderBy("tipo_dato").show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consolidación (groupBy) para variables

# COMMAND ----------

grp_cols = ["zona_id", "timestamp_forecast", "lat", "lon", "run_ts", "source"]

df_fact = df_base.groupBy(*grp_cols).agg(
    F.max("thetao").alias("temperatura_mar_c"),
    F.max("so").alias("salinidad_psu"),
    F.max("uo").alias("corriente_u_ms"),
    F.max("vo").alias("corriente_v_ms"),
    F.max("created_at").alias("created_at"),
)

print("Fact consolidado creado")
print("Registros:", df_fact.count())
display(df_fact.limit(10))


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Filtrar filas vacías

# COMMAND ----------

df_fact = df_fact.filter(
    F.col("temperatura_mar_c").isNotNull() |
    F.col("salinidad_psu").isNotNull() |
    F.col("corriente_u_ms").isNotNull() |
    F.col("corriente_v_ms").isNotNull()
)

print("Fact filtrado (sin filas vacías)")
print("Registros:", df_fact.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validaciones de calidad

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Validación de claves

# COMMAND ----------

null_keys = df_fact.filter(
    F.col("zona_id").isNull() |
    F.col("timestamp_forecast").isNull() |
    F.col("lat").isNull() |
    F.col("lon").isNull() |
    F.col("run_ts").isNull()
)

print("Filas con claves nulas:", null_keys.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Validación de rangos físicos

# COMMAND ----------

invalid = df_fact.filter(
    (F.col("temperatura_mar_c").isNotNull() &
     ((F.col("temperatura_mar_c") < -2) | (F.col("temperatura_mar_c") > 35))) |

    (F.col("salinidad_psu").isNotNull() &
     ((F.col("salinidad_psu") < 20) | (F.col("salinidad_psu") > 45))) |

    (F.col("corriente_u_ms").isNotNull() &
     (F.abs(F.col("corriente_u_ms")) > 5)) |

    (F.col("corriente_v_ms").isNotNull() &
     (F.abs(F.col("corriente_v_ms")) > 5))
)

print("Filas fuera de rango:", invalid.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 — Duplicados por clave primaria

# COMMAND ----------

dup = df_fact.groupBy(
    "zona_id",
    "timestamp_forecast",
    "lat",
    "lon",
    "run_ts"
).count().filter(F.col("count") > 1)

print("Claves duplicadas:", dup.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Crear tabla Silver (DDL)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS adb_ecoazul.silver.fact_copernicus_forecast (
  zona_id STRING NOT NULL,
  timestamp_forecast TIMESTAMP NOT NULL,
  run_ts TIMESTAMP NOT NULL,

  run_date DATE GENERATED ALWAYS AS (CAST(run_ts AS DATE)),

  lat DOUBLE NOT NULL,
  lon DOUBLE NOT NULL,

  temperatura_mar_c DOUBLE,
  salinidad_psu DOUBLE,
  corriente_u_ms DOUBLE,
  corriente_v_ms DOUBLE,

  source STRING NOT NULL,
  created_at TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (run_date)
""")

print("Tabla creada/verificada: adb_ecoazul.silver.fact_copernicus_forecast")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 MERGE (UPSERT) a Silver

# COMMAND ----------

df_fact.createOrReplaceTempView("tmp_copernicus_new")
print("Vista temporal creada: tmp_copernicus_new")


# COMMAND ----------

spark.sql("""
MERGE INTO adb_ecoazul.silver.fact_copernicus_forecast AS target
USING tmp_copernicus_new AS source
ON  target.zona_id = source.zona_id
AND target.timestamp_forecast = source.timestamp_forecast
AND target.lat = source.lat
AND target.lon = source.lon
AND target.run_ts = source.run_ts
WHEN MATCHED THEN UPDATE SET
  target.temperatura_mar_c = source.temperatura_mar_c,
  target.salinidad_psu = source.salinidad_psu,
  target.corriente_u_ms = source.corriente_u_ms,
  target.corriente_v_ms = source.corriente_v_ms,
  target.source = source.source,
  target.created_at = source.created_at
WHEN NOT MATCHED THEN INSERT (
  zona_id, timestamp_forecast, run_ts, lat, lon,
  temperatura_mar_c, salinidad_psu, corriente_u_ms, corriente_v_ms,
  source, created_at
) VALUES (
  source.zona_id, source.timestamp_forecast, source.run_ts, source.lat, source.lon,
  source.temperatura_mar_c, source.salinidad_psu, source.corriente_u_ms, source.corriente_v_ms,
  source.source, source.created_at
)
""")

print("MERGE completado: fact_copernicus_forecast")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 Post-check rápido

# COMMAND ----------

spark.sql("""
SELECT
  COUNT(*) AS total_registros,
  COUNT(DISTINCT zona_id) AS zonas,
  MIN(timestamp_forecast) AS min_ts,
  MAX(timestamp_forecast) AS max_ts,
  MIN(run_ts) AS min_run,
  MAX(run_ts) AS max_run
FROM adb_ecoazul.silver.fact_copernicus_forecast
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Optimización

# COMMAND ----------

spark.sql("""
OPTIMIZE adb_ecoazul.silver.fact_copernicus_forecast
ZORDER BY (zona_id, timestamp_forecast)
""")

spark.sql("""
VACUUM adb_ecoazul.silver.fact_copernicus_forecast RETAIN 168 HOURS
""")

print("OPTIMIZE y VACUUM completados")


# COMMAND ----------

# MAGIC %md
