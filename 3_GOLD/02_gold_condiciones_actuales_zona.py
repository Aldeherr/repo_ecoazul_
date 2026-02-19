# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Imports + parámetros + nombres

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "adb_ecoazul"
GOLD = f"{CATALOG}.gold"

SOURCE_TABLE = f"{GOLD}.forecast_zona_hora"
TARGET_TABLE = f"{GOLD}.condiciones_actuales_zona"

CURRENT_WINDOW_HOURS = 3
print("SOURCE_TABLE:", SOURCE_TABLE)
print("TARGET_TABLE:", TARGET_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.  Crear tabla target (DDL)

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  zona_id STRING NOT NULL,
  timestamp_forecast_actual TIMESTAMP NOT NULL,

  wave_height_m DOUBLE,
  wind_speed_ms DOUBLE,
  wind_speed_kts DOUBLE,

  precipitation_mm DOUBLE,
  cloud_cover_pct DOUBLE,
  visibility_km DOUBLE,

  altura_marea_m DOUBLE,
  tipo_proximo_cambio STRING,

  temperatura_mar_c DOUBLE,
  salinidad_psu DOUBLE,
  corriente_u_ms DOUBLE,
  corriente_v_ms DOUBLE,
  corriente_speed_ms DOUBLE,

  risk_score DOUBLE,
  safety_level STRING,
  alerta_texto STRING,

  source_updated_at TIMESTAMP,
  updated_at TIMESTAMP NOT NULL,
  minutes_since_update INT,

  CONSTRAINT pk_condiciones PRIMARY KEY (zona_id)
) USING DELTA
""")

# Si la tabla ya existe, agregar columnas nuevas (idempotente)
for col_name, col_type in [("precipitation_mm", "DOUBLE"), ("cloud_cover_pct", "DOUBLE")]:
    try:
        spark.sql(f"ALTER TABLE {TARGET_TABLE} ADD COLUMN {col_name} {col_type}")
        print(f"Columna agregada: {col_name}")
    except Exception:
        pass  # Ya existe

print("Tabla creada/verificada:", TARGET_TABLE)


# COMMAND ----------

spark.sql(f"DESCRIBE {TARGET_TABLE}").show(200, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.  Leer base + filtrar ventana “actual”

# COMMAND ----------

df = spark.table(SOURCE_TABLE)

now_ts = F.current_timestamp()
window_end = F.expr(f"current_timestamp() + INTERVAL {CURRENT_WINDOW_HOURS} HOURS")

df_win = df.where(
    (F.col("timestamp_forecast") >= now_ts) &
    (F.col("timestamp_forecast") < window_end)
)

print("Filas en ventana:", df_win.count())
df_win.select("zona_id", "timestamp_forecast", "updated_at").orderBy("zona_id", "timestamp_forecast").show(20, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.  Elegir “la hora actual” por zona (primer timestamp >= now)

# COMMAND ----------

w = Window.partitionBy("zona_id").orderBy(F.col("timestamp_forecast").asc())

df_pick = (
    df_win
    .withColumn("rn", F.row_number().over(w))
    .where(F.col("rn") == 1)
    .drop("rn")
)

print("Zonas seleccionadas:", df_pick.select("zona_id").distinct().count())
df_pick.select("zona_id", "timestamp_forecast", "wave_height_m", "wind_speed_ms", "altura_marea_m", "temperatura_mar_c", "updated_at") \
    .orderBy("zona_id").show(50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.  Derivadas (kts + speed corriente)

# COMMAND ----------

df_feat = (
    df_pick
    .withColumn("timestamp_forecast_actual", F.col("timestamp_forecast"))
    .withColumn("wind_speed_kts", F.col("wind_speed_ms") * F.lit(1.94384))
    .withColumn(
        "corriente_speed_ms",
        F.sqrt(F.pow(F.col("corriente_u_ms"), 2) + F.pow(F.col("corriente_v_ms"), 2))
    )
)

df_feat.select(
    "zona_id",
    "timestamp_forecast_actual",
    "wind_speed_ms", "wind_speed_kts",
    "corriente_u_ms", "corriente_v_ms", "corriente_speed_ms"
).orderBy("zona_id").show(50, truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6.  risk_score, safety_level, alerta_texto

# COMMAND ----------

df_feat.printSchema()

# COMMAND ----------

df_scored = (
    df_feat
    .withColumn(
        "risk_score",
        F.coalesce(F.col("wave_height_m"), F.lit(0.0)) * F.lit(10.0) +
        F.coalesce(F.col("wind_speed_kts"), F.lit(0.0)) +
        F.coalesce(F.col("corriente_speed_ms"), F.lit(0.0)) * F.lit(5.0) +
        F.when(F.coalesce(F.col("visibility_km"), F.lit(100.0)) < F.lit(2.0), F.lit(20.0)).otherwise(F.lit(0.0))
    )
    .withColumn(
        "safety_level",
        F.when(
            (F.coalesce(F.col("wave_height_m"), F.lit(0.0)) >= F.lit(2.5)) |
            (F.coalesce(F.col("wind_speed_kts"), F.lit(0.0)) >= F.lit(25.0)) |
            (F.coalesce(F.col("visibility_km"), F.lit(100.0)) < F.lit(1.0)),
            F.lit("PELIGRO")
        ).when(
            F.col("risk_score") >= F.lit(40.0),
            F.lit("PELIGRO")
        ).when(
            F.col("risk_score") >= F.lit(20.0),
            F.lit("PRECAUCION")
        ).otherwise(
            F.lit("SEGURO")
        )
    )
    .withColumn(
        "alerta_texto",
        F.when(F.coalesce(F.col("wave_height_m"), F.lit(0.0)) >= F.lit(2.5), F.lit("Oleaje alto"))
         .when(F.coalesce(F.col("wind_speed_kts"), F.lit(0.0)) >= F.lit(25.0), F.lit("Viento fuerte"))
         .when(F.col("risk_score") >= F.lit(40.0), F.lit("Condiciones riesgosas"))
         .when(F.col("risk_score") >= F.lit(20.0), F.lit("Precaucion recomendada"))
         .otherwise(F.lit(None))
    )
    .withColumn("source_updated_at", F.col("updated_at"))
    .withColumn(
        "minutes_since_update",
        F.round(
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.col("source_updated_at"))) / 60.0, 
            0
        ).cast("int")
    )
    .withColumn("updated_at", now_ts)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7.  Preparar DF final para MERGE (columnas exactas)

# COMMAND ----------

df_out = df_scored.select(
    "zona_id",
    "timestamp_forecast_actual",

    "wave_height_m",
    "wind_speed_ms",
    "wind_speed_kts",

    "precipitation_mm",
    "cloud_cover_pct",
    "visibility_km",

    "altura_marea_m",
    "tipo_proximo_cambio",

    "temperatura_mar_c",
    "salinidad_psu",
    "corriente_u_ms",
    "corriente_v_ms",
    "corriente_speed_ms",

    "risk_score",
    "safety_level",
    "alerta_texto",

    "source_updated_at",
    "updated_at",
    "minutes_since_update",
)

print("Filas a escribir:", df_out.count())
# COMMAND ----------

# MAGIC %md
# MAGIC ## 8.  MERGE a gold.condiciones_actuales_zona

# COMMAND ----------

df_out.createOrReplaceTempView("tmp_condiciones_actuales_zona")

spark.sql(f"""
MERGE INTO {TARGET_TABLE} AS t
USING tmp_condiciones_actuales_zona AS s
ON t.zona_id = s.zona_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("MERGE completado:", TARGET_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9.  Post-check

# COMMAND ----------

EXPECTED_ZONAS = ["BT01","CO01","GG01","PVX01","BH01","GM01","PD01","GP01","PCH01","GCH01"]
df_target = spark.table(TARGET_TABLE)

missing = (
    spark.createDataFrame([(z,) for z in EXPECTED_ZONAS], ["zona_id"])
    .join(df_target.select("zona_id").distinct(), on="zona_id", how="left_anti")
)

missing_list = [r["zona_id"] for r in missing.collect()]

if missing_list:
    raise Exception(f"POST-CHECK FAILED: faltan zonas en {TARGET_TABLE}: {missing_list}")

print(f"POST-CHECK OK: {df_target.select('zona_id').distinct().count()} zonas presentes")
