# Databricks notebook source
# MAGIC %md
# MAGIC ## CONFIGURACION INICIAL

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from databricks.sdk.runtime import dbutils
from pyspark.sql import Row
from pytz import timezone

STORAGE_ACCOUNT = "dataecoazul"
BRONZE_ROOT = f"wasbs://bronze@{STORAGE_ACCOUNT}.blob.core.windows.net"
STORAGE_KEY = dbutils.secrets.get(scope="keyvault-scope", key="storage-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    STORAGE_KEY
)

sources = {
    "stormglass": f"{BRONZE_ROOT}/stormglass/",
    "marea": f"{BRONZE_ROOT}/marea/",
    "copernicus": f"{BRONZE_ROOT}/copernicus/",
}


# COMMAND ----------

BACKDAYS = 2  # cantidad de días hacia atrás que quieres analizar
cutoff_utc = datetime.utcnow() - timedelta(days=BACKDAYS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VERIFICABLES

# COMMAND ----------

def lsR_files(path: str):
    """
    Recorre recursivamente un path en DBFS y retorna los FileInfo de todos los archivos .parquet.

    Silencia errores de acceso a subdirectorios para no interrumpir el escaneo completo.

    Returns:
        list de FileInfo objects con atributos path, modificationTime y size.
    """
    out = []
    try:
        for fi in dbutils.fs.ls(path):
            if fi.isFile():
                if fi.path.lower().endswith(".parquet"):
                    out.append(fi)
            else:
                out.extend(lsR_files(fi.path))
    except Exception:
        pass
    return out

rows = []
for source, base in sources.items():
    files = lsR_files(base)
    for fi in files:

        # convertir modificationTime a datetime UTC
        file_utc = datetime.utcfromtimestamp(fi.modificationTime / 1000)

        # filtrar por BACKDAYS
        if file_utc < cutoff_utc:
            continue

        path = fi.path.lower()

        # Clasificación forecast vs historico basada en la ruta
        if "/forecast/" in path:
            variante = "forecast"
        elif "/historico/" in path or "/history/" in path:
            variante = "historico"
        else:
            variante = "desconocido"

        rows.append(
            Row(
                source=source,
                variante=variante,
                path=fi.path,
                modTimeMs=fi.modificationTime
            )
        )

df = spark.createDataFrame(rows) \
    .withColumn("written_utc", (F.col("modTimeMs") / 1000).cast("timestamp")) \
    .withColumn("written_pty", F.from_utc_timestamp("written_utc", "America/Panama")) \
    .withColumn("written_pty_str", F.date_format("written_pty", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("written_utc_str", F.date_format("written_utc", "yyyy-MM-dd HH:mm:ss"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## FILTRAR HORARIOS

# COMMAND ----------

# VARIANTE = 'forecast'
# SOURCE_FILTER = 'marea'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conteo por día

# COMMAND ----------

df_day = df.withColumn("dia_pty", F.to_date("written_pty"))

df_day.groupBy("source", "variante", "dia_pty") \
      .count() \
      .orderBy(F.col("dia_pty").desc(), "source", "variante") \
      .show(200, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Conteo por día con horas como columnas (pivot)

# COMMAND ----------

df_hour = df.withColumn("dia_pty", F.to_date("written_pty")) \
            .withColumn("hora_pty", F.date_format("written_pty", "HH"))

# Si quieres por una fuente específica, descomenta:
# df_hour = df_hour.filter((F.col("source") == "stormglass") & (F.col("variante") == "forecast"))

pivot_hours = [f"{h:02d}" for h in range(24)]

df_pivot = df_hour.groupBy("source", "variante", "dia_pty") \
    .pivot("hora_pty", pivot_hours) \
    .count() \
    .fillna(0) \
    .orderBy(F.col("dia_pty").desc(), "source", "variante")

# columnas fijas que siempre se conservan
fixed_cols = ["source", "variante", "dia_pty"]

# columnas de horas (las del pivot)
hour_cols = [c for c in df_pivot.columns if c not in fixed_cols]

# detectar columnas que tienen al menos un valor > 0
non_empty_hour_cols = [
    c for c in hour_cols
    if df_pivot.filter(F.col(c) > 0).limit(1).count() > 0
]

# seleccionar solo columnas útiles
df_pivot_clean = df_pivot.select(*(fixed_cols + non_empty_hour_cols))

df_pivot_clean.show(200, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC # ESCRITURA DE BRONZE_FILE_LANDED

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS adb_ecoazul.dq")

spark.sql("""
CREATE TABLE IF NOT EXISTS adb_ecoazul.dq.bronze_file_landed (
  source STRING,
  variante STRING,
  path STRING,
  modTimeMs BIGINT,
  written_utc TIMESTAMP,
  written_pty TIMESTAMP,
  dia_pty DATE,
  hora_pty STRING,
  written_pty_str STRING,
  written_utc_str STRING,
  captured_at TIMESTAMP
) USING DELTA
PARTITIONED BY (dia_pty)
""")


# COMMAND ----------

# MAGIC %md
# MAGIC # GUARDAR DATASET

# COMMAND ----------

from pyspark.sql import functions as F

df_out = df.select(
    "source",
    "variante",
    "path",
    "modTimeMs",
    "written_utc",
    "written_pty",
    F.to_date("written_pty").alias("dia_pty"),
    F.date_format("written_pty", "HH").alias("hora_pty"),
    "written_pty_str",
    "written_utc_str",
).withColumn("captured_at", F.current_timestamp())

# Idempotencia simple: borrar días del rango BACKDAYS y reinsertar
pty = timezone("America/Panama")
start_pty = datetime.now(pty) - timedelta(days=BACKDAYS)
start_pty_str = start_pty.strftime("%Y-%m-%d")

spark.sql(f"""
DELETE FROM adb_ecoazul.dq.bronze_file_landed
WHERE dia_pty >= DATE('{start_pty_str}')
""")

df_out.write.mode("append").saveAsTable("adb_ecoazul.dq.bronze_file_landed")

print("Append completado a adb_ecoazul.dq.bronze_file_landed desde", start_pty_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Últimas llegadas por fuente y variante

# COMMAND ----------

spark.sql("""
SELECT
  source,
  variante,
  MAX(written_pty) AS last_written_pty
FROM adb_ecoazul.dq.bronze_file_landed
GROUP BY source, variante
ORDER BY source, variante
""").show(truncate=False)


# COMMAND ----------

# MAGIC %md
