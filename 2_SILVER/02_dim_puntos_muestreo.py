# Databricks notebook source
# MAGIC %md
# MAGIC # USAR CATALOGO

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adb_ecoazul;
# MAGIC USE SCHEMA silver;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLA DESTINO

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_ecoazul.silver.dim_puntos_muestreo (
# MAGIC   punto_id STRING,
# MAGIC   zona_id STRING,
# MAGIC   zona_nombre STRING,
# MAGIC   tipo_punto STRING,
# MAGIC   lat DOUBLE,
# MAGIC   lon DOUBLE,
# MAGIC   region STRING,
# MAGIC   provincias STRING,
# MAGIC   activo BOOLEAN,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## SAS PARA ACCEDER A CONFIG_APIS.JSON

# COMMAND ----------

import requests, json
from pyspark.sql import functions as F

STORAGE_ACCOUNT = "dataecoazul"
CONTAINER = "config"
BLOB_PATH = "ecoazul/config/config_apis.json"

SAS = dbutils.secrets.get(scope="keyvault-scope", key="config-blob-sas")  # recomendado

CONFIG_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER}/{BLOB_PATH}{SAS}"

resp = requests.get(CONFIG_URL, timeout=30)
resp.raise_for_status()
cfg = resp.json()


# COMMAND ----------

puntos_raw = cfg["puntos_muestreo"]  # lista de dicts

df = spark.createDataFrame(cfg["puntos_muestreo"]).select(
    F.col("id").alias("punto_id"),          # o si ya es punto_id, déjalo directo
    F.col("zona_id"),
    F.col("zona_nombre"),
    F.col("tipo_punto"),
     F.col("lat").cast("double").alias("lat"),
    F.col("lng").cast("double").alias("lon"),  # si ya es lon, usa lon
    F.col("region"),
    F.col("provincias"),
    F.coalesce(F.col("activo").cast("boolean"), F.lit(True)).alias("activo"),
    F.current_timestamp().alias("updated_at"),
)

# Validaciones mínimas
bad = df.filter(F.col("punto_id").isNull() | F.col("zona_id").isNull())
if bad.count() > 0:
    display(bad)
    raise Exception("Hay registros sin punto_id o zona_id en puntosmuestreo.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE (upsert)

# COMMAND ----------

df.createOrReplaceTempView("tmp_dim_puntos_muestreo")

spark.sql("""
MERGE INTO adb_ecoazul.silver.dim_puntos_muestreo AS t
USING tmp_dim_puntos_muestreo AS s
ON t.punto_id = s.punto_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## CHECKS (confirmar interior + respaldo)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   COUNT(*) AS n_puntos,
# MAGIC   SUM(CASE WHEN tipo_punto = 'interior' THEN 1 ELSE 0 END) AS n_interior
# MAGIC FROM adb_ecoazul.silver.dim_puntos_muestreo
# MAGIC WHERE activo = true
# MAGIC GROUP BY zona_id
# MAGIC ORDER BY zona_id;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ZONAS HUERFANAS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.zona_id, COUNT(*) AS n
# MAGIC FROM adb_ecoazul.silver.dim_puntos_muestreo p
# MAGIC LEFT ANTI JOIN adb_ecoazul.silver.dim_zonas z
# MAGIC ON p.zona_id = z.zona_id
# MAGIC GROUP BY p.zona_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   COUNT(*) AS n_puntos,
# MAGIC   SUM(CASE WHEN tipo_punto = 'interior' THEN 1 ELSE 0 END) AS n_interior
# MAGIC FROM adb_ecoazul.silver.dim_puntos_muestreo
# MAGIC WHERE activo = true
# MAGIC GROUP BY zona_id
# MAGIC ORDER BY zona_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT punto_id, COUNT(*) c
# MAGIC FROM adb_ecoazul.silver.dim_puntos_muestreo
# MAGIC GROUP BY punto_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC