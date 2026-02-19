# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG adb_ecoazul;
# MAGIC CREATE SCHEMA IF NOT EXISTS adb_ecoazul.silver;
# MAGIC USE SCHEMA silver;
# MAGIC

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

puntos_pesca = [
    {"id":"BT01","nombre":"Bocas del Toro (continental)","lat":9.3985776,"lng":-82.1923224,"provincias":"Bocas del Toro"},
    {"id":"CO01","nombre":"Costa Abajo Colón","lat":9.2360403,"lng":-80.1992382,"provincias":"Colón"},
    {"id":"GG01","nombre":"Golfo de Panamá – líneas costeras","lat":8.8,"lng":-79.7,"provincias":"Panamá"},
    {"id":"PVX01","nombre":"Pixvae","lat":7.8351394,"lng":-81.6408541,"provincias":"Veraguas"},
    {"id":"BH01","nombre":"Bahía Honda","lat":7.6762836,"lng":-81.47205,"provincias":"Veraguas"},
    {"id":"GM01","nombre":"Golfo Montijo","lat":7.6932686,"lng":-81.1180117,"provincias":"Veraguas"},
    {"id":"PD01","nombre":"Pedasí","lat":7.3992057,"lng":-80.1020331,"provincias":"Los Santos"},
    {"id":"GP01","nombre":"Bahía de Parita","lat":8.1630421,"lng":-80.3990094,"provincias":"Herrera"},
    {"id":"PCH01","nombre":"Punta Chame","lat":8.5706506,"lng":-79.7315935,"provincias":"Pma. Oeste"},
    {"id":"GCH01","nombre":"Golfo Chiriquí","lat":8.2245103,"lng":-82.4499123,"provincias":"Chiriquí"},
]

OFFSET = 0.25

rows = []
now = datetime.utcnow()

for p in puntos_pesca:
    zona_id = p["id"]
    zona_nombre = p["nombre"]
    lat_centro = float(p["lat"])
    lon_centro = float(p["lng"])  # normalizamos: lon_*, no lng_*
    provincias = p.get("provincias")

    if zona_id == "GM01":
        lat_min, lat_max = 7.35, 7.95
        lon_min, lon_max = -81.35, -80.97  # normalizamos a lon_*
    else:
        lat_min, lat_max = lat_centro - OFFSET, lat_centro + OFFSET
        lon_min, lon_max = lon_centro - OFFSET, lon_centro + OFFSET

    rows.append((
        zona_id, zona_nombre, provincias,
        lat_centro, lon_centro,
        lat_min, lat_max, lon_min, lon_max,
        "copernicus_bbox", True, now
    ))

schema = StructType([
    StructField("zona_id", StringType(), False),
    StructField("zona_nombre", StringType(), False),
    StructField("provincias", StringType(), True),
    StructField("lat_centro", DoubleType(), False),
    StructField("lon_centro", DoubleType(), False),
    StructField("lat_min", DoubleType(), False),
    StructField("lat_max", DoubleType(), False),
    StructField("lon_min", DoubleType(), False),
    StructField("lon_max", DoubleType(), False),
    StructField("fuente_geometria", StringType(), False),
    StructField("activo", BooleanType(), False),
    StructField("updated_at", TimestampType(), False),
])

df = spark.createDataFrame(rows, schema=schema)

# Validaciones rápidas de bbox
bad = df.filter((F.col("lat_min") >= F.col("lat_max")) | (F.col("lon_min") >= F.col("lon_max")))
if bad.count() > 0:
    display(bad)
    raise Exception("Hay zonas con bbox inválido (min >= max). Revisa coordenadas/offset.")


# COMMAND ----------



# COMMAND ----------

df.createOrReplaceTempView("tmp_dim_zonas")

spark.sql("""
MERGE INTO adb_ecoazul.silver.dim_zonas AS t
USING tmp_dim_zonas AS s
ON t.zona_id = s.zona_id
WHEN MATCHED THEN UPDATE SET
  t.zona_nombre = s.zona_nombre,
  t.lat_centro = s.lat_centro,
  t.lon_centro = s.lon_centro,
  t.lat_min = s.lat_min,
  t.lat_max = s.lat_max,
  t.lon_min = s.lon_min,
  t.lon_max = s.lon_max,
  t.fuente_geometria = s.fuente_geometria,
  t.activo = s.activo,
  t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS n, COUNT(DISTINCT zona_id) AS distinct_n
# MAGIC FROM adb_ecoazul.silver.dim_zonas;
# MAGIC
# MAGIC SELECT zona_id, zona_nombre, lat_centro, lon_centro, lat_min, lat_max, lon_min, lon_max
# MAGIC FROM adb_ecoazul.silver.dim_zonas
# MAGIC ORDER BY zona_id;
# MAGIC