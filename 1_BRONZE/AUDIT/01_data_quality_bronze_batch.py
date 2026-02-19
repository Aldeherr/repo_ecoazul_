# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Bronze — Batch Driver (sin widgets)
# MAGIC
# MAGIC Ejecutar por schedule (Workflows Job).
# MAGIC Valida archivos recientes aterrizados en Bronze que aún no han sido validados.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# CONFIG FIJA (edítala aquí)

LANDED_TABLE = "adb_ecoazul.dq.bronze_file_landed"
VALIDATION_TABLE = "adb_ecoazul.dq.validation_results"

# Ruta del NOTEBOOK validador (worker) en tu workspace
# USAR PATH ABSOLUTO: abrir notebook en Databricks → clic en nombre → "Copy path"
VALIDATOR_NOTEBOOK_PATH = "/Users/aldeherr@ucm.es/1_BRONZE/AUDIT/01_data_quality_bronze"

# Ventana de búsqueda de pendientes
LOOKBACK_HOURS = 24

# Control de costo: máximo de archivos por fuente por corrida
MAX_FILES_PER_SOURCE = 15

# Fuentes soportadas por el worker (debe coincidir con su CONTRACT_FP)
SOURCES = ["marea", "stormglass", "copernicus"]

# Nombre de columnas en bronze_file_landed (ajusta si difieren)
COL_SOURCE = "source"
COL_PATH = "path"
COL_EVENT_TIME = "written_utc"   # si en tu tabla se llama distinto, cámbialo aquí

# COMMAND ----------

STORAGE_ACCOUNT = "dataecoazul"
BRONZE_ROOT = f"wasbs://bronze@{STORAGE_ACCOUNT}.blob.core.windows.net"
STORAGE_KEY = dbutils.secrets.get(scope="keyvault-scope", key="storage-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    STORAGE_KEY
)

# COMMAND ----------

# LOGIC

print(f"[DQ BATCH] LANDED_TABLE={LANDED_TABLE}")
print(f"[DQ BATCH] VALIDATION_TABLE={VALIDATION_TABLE}")
print(f"[DQ BATCH] VALIDATOR_NOTEBOOK_PATH={VALIDATOR_NOTEBOOK_PATH}")
print(f"[DQ BATCH] LOOKBACK_HOURS={LOOKBACK_HOURS} MAX_FILES_PER_SOURCE={MAX_FILES_PER_SOURCE} SOURCES={SOURCES}")

# 1) candidatos recientes
landed = (
    spark.table(LANDED_TABLE)
         .where(F.col(COL_EVENT_TIME) >= F.expr(f"current_timestamp() - INTERVAL {LOOKBACK_HOURS} HOURS"))
         .select(
             F.lower(F.col(COL_SOURCE)).alias("source"),
             F.col(COL_PATH).alias("path"),
             F.col(COL_EVENT_TIME).alias("event_time")
         )
         .where(F.col("source").isin(SOURCES))
)

# 2) ya validados (por path)
validated = spark.table(VALIDATION_TABLE).select("path").distinct()

# 3) pendientes = landed - validated
pending = (
    landed.join(validated, on="path", how="left_anti")
          .orderBy(F.col("event_time").desc())
)

# 4) limitar por fuente
w = Window.partitionBy("source").orderBy(F.col("event_time").desc())
pending_limited = (
    pending.withColumn("rn", F.row_number().over(w))
           .where(F.col("rn") <= MAX_FILES_PER_SOURCE)
           .drop("rn")
)

rows = pending_limited.collect()
print(f"[DQ BATCH] pendientes={len(rows)}")

# 5) ejecutar worker por archivo
fails = 0
for r in rows:
    src = r["source"]
    pth = r["path"]
    try:
        dbutils.notebook.run(
            VALIDATOR_NOTEBOOK_PATH,
            timeout_seconds=0,
            arguments={"source": src, "path": pth}
        )
        print(f"[DQ BATCH][PASS] {src} {pth}")
    except Exception as e:
        # No tumbar todo por un fallo: seguir y reportar
        print(f"[DQ BATCH][FAIL] {src} {pth} -> {e}")
        fails += 1

if fails > 0:
    raise Exception(f"[DQ BATCH] terminado con fails={fails}. Revisa {VALIDATION_TABLE}")

print("[DQ BATCH] OK (sin fallos)")