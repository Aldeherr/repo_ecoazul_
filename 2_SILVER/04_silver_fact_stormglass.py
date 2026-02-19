# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - SILVER FACT: StormGlass Forecast
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar datos crudos de Bronze (parquets de StormGlass) a tabla Silver normalizada para análisis.
# MAGIC
# MAGIC ## Estrategia de Ingesta
# MAGIC - **Frecuencia**: 3 corridas/día (04:00, 12:00, 18:00 Panamá)
# MAGIC - **Cobertura**: 10 zonas × 3 puntos estratégicos = 30 puntos por corrida
# MAGIC - **Forecast**: 72 horas adelante
# MAGIC - **Consumo API**: 90 requests/día (18% del límite de 500)
# MAGIC
# MAGIC ## Inputs
# MAGIC - **Bronze**: `wasbs://bronze@dataecoazul/stormglass/forecast/` (parquets escritos por Azure Functions)
# MAGIC - **Estructura Bronze**: Cada parquet contiene `raw_data.hours[]` con forecast completo
# MAGIC
# MAGIC ## Outputs
# MAGIC - **Silver**: `adb_ecoazul.silver.fact_stormglass_forecast` (Delta table)
# MAGIC - **Granularidad**: 1 registro por (punto_id, hora_forecast, run_ts)
# MAGIC - **Volumen estimado**: ~8,640 registros/día (30 puntos × 48h × 6 corridas)
# MAGIC
# MAGIC ## Transformaciones
# MAGIC 1. Lectura de Bronze (últimos N días)
# MAGIC 2. Explosión de `raw_data.hours[]` para generar 1 fila por hora de forecast
# MAGIC 3. Extracción de valores desde nested dicts (sg/noaa/icon) con coalesce
# MAGIC 4. Normalización: snake_case, tipos correctos
# MAGIC 5. Validaciones de calidad (rangos, nulls, duplicados)
# MAGIC 6. Merge a Silver (upsert por clave primaria)
# MAGIC
# MAGIC ## Validaciones
# MAGIC - No nulls en claves (punto_id, zona_id, timestamp_forecast, run_ts)
# MAGIC - Rangos válidos (wave_height 0-20m, wind_speed 0-50m/s, etc.)
# MAGIC - No duplicados por (punto_id, timestamp_forecast, run_ts)
# MAGIC - FK válidas (zona_id existe en dim_zonas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adb_ecoazul;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta

# Configuración de acceso a Bronze
STORAGE_ACCOUNT = "dataecoazul"
STORAGE_KEY = dbutils.secrets.get(scope="keyvault-scope", key="storage-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    STORAGE_KEY
)

BRONZE_BASE = f"wasbs://bronze@{STORAGE_ACCOUNT}.blob.core.windows.net/stormglass/"

print(f"Bronze path: {BRONZE_BASE}")

# COMMAND ----------

def lsR_parquet(path):
    """Lista recursivamente todos los archivos .parquet bajo un path de DBFS.

    Recorre subdirectorios de forma recursiva. Los errores de acceso se ignoran
    silenciosamente para no interrumpir el pipeline.

    Args:
        path: Ruta de DBFS a explorar (e.g., wasbs://bronze@...).

    Returns:
        Lista de strings con las rutas absolutas de los parquets encontrados.
    """
    out = []
    for fi in dbutils.fs.ls(path):
        if fi.isFile():
            if fi.path.lower().endswith(".parquet"):
                out.append(fi.path)
        else:
            out.extend(lsR_parquet(fi.path))
    return out

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear Tabla Silver (si no existe)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_ecoazul.silver.fact_stormglass_forecast (
# MAGIC   punto_id STRING NOT NULL COMMENT 'ID del punto de muestreo (ej: BT01_interior)',
# MAGIC   zona_id STRING NOT NULL COMMENT 'FK a dim_zonas',
# MAGIC   timestamp_forecast TIMESTAMP NOT NULL COMMENT 'Hora predicha del forecast',
# MAGIC   run_ts TIMESTAMP NOT NULL COMMENT 'Timestamp de la corrida que generó el forecast',
# MAGIC   
# MAGIC    -- Partición (columna física generada)
# MAGIC   run_date DATE GENERATED ALWAYS AS (CAST(run_ts AS DATE)),
# MAGIC
# MAGIC   -- Oleaje (Wave)
# MAGIC   wave_height_m DOUBLE COMMENT 'Altura significativa de ola (metros)',
# MAGIC   wave_direction_deg DOUBLE COMMENT 'Dirección del oleaje (grados, 0-360)',
# MAGIC   wave_period_s DOUBLE COMMENT 'Período de ola (segundos)',
# MAGIC   
# MAGIC   -- Oleaje de fondo (Swell)
# MAGIC   swell_height_m DOUBLE COMMENT 'Altura del swell (metros)',
# MAGIC   swell_direction_deg DOUBLE COMMENT 'Dirección del swell (grados)',
# MAGIC   swell_period_s DOUBLE COMMENT 'Período del swell (segundos)',
# MAGIC   
# MAGIC   -- Viento (Wind)
# MAGIC   wind_speed_ms DOUBLE COMMENT 'Velocidad del viento (m/s)',
# MAGIC   wind_direction_deg DOUBLE COMMENT 'Dirección del viento (grados)',
# MAGIC   gust_ms DOUBLE COMMENT 'Ráfagas de viento (m/s)',
# MAGIC   
# MAGIC   -- Atmósfera
# MAGIC   air_temperature_c DOUBLE COMMENT 'Temperatura del aire (°C)',
# MAGIC   pressure_hpa DOUBLE COMMENT 'Presión atmosférica (hPa)',
# MAGIC   precipitation_mm DOUBLE COMMENT 'Precipitación (mm)',
# MAGIC   cloud_cover_pct DOUBLE COMMENT 'Cobertura de nubes (%, 0-100)',
# MAGIC   visibility_km DOUBLE COMMENT 'Visibilidad horizontal (km)',
# MAGIC
# MAGIC   -- Metadata
# MAGIC   source STRING NOT NULL COMMENT 'Fuente del dato (stormglass)',
# MAGIC   created_at TIMESTAMP NOT NULL COMMENT 'Timestamp de inserción en Silver',
# MAGIC   
# MAGIC   CONSTRAINT pk_stormglass PRIMARY KEY (punto_id, timestamp_forecast, run_ts)
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC COMMENT 'Forecast oceánico y atmosférico de StormGlass API. 6 corridas/día × 30 puntos × 48h forecast';

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de Bronze
# MAGIC
# MAGIC **Estrategia**: Leer los últimos N días de Bronze para reprocesar/actualizar Silver.
# MAGIC
# MAGIC Puedes cambiar `DAYS_LOOKBACK` según tu necesidad:
# MAGIC - `1` = solo hoy (incremental diario)
# MAGIC - `7` = última semana (reproceso semanal)
# MAGIC - `None` = todo Bronze (carga histórica completa)

# COMMAND ----------

# Parámetros de lectura
DAYS_LOOKBACK = 2  # Cambiar a None para procesar todo Bronze

# Listar archivos en Bronze
try:
    all_files = lsR_parquet(BRONZE_BASE)
    parquet_files = [f for f in all_files if f.endswith(".parquet")]
    
    if not parquet_files:
        raise Exception(f" No se encontraron parquets en {BRONZE_BASE}")
    
    print(f" Encontrados {len(parquet_files)} archivos parquet en Bronze")
    
    # Filtrar por fecha si es incremental
    if DAYS_LOOKBACK:
        from datetime import datetime, timedelta
        
        # Generar lista de fechas válidas
        valid_dates = [
            (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(DAYS_LOOKBACK + 1)
        ]
        
        parquet_files = []
        for date_str in valid_dates:
            year = date_str[:4]
            month = date_str[5:7]
            day = date_str[8:10]
            
            # Buscar archivos específicos de esa fecha
            date_path = f"{BRONZE_BASE}year={year}/month={month}/forecast/day={day}/"
            try:
                date_files = [f.path for f in dbutils.fs.ls(date_path) if f.path.endswith(".parquet")]
                parquet_files.extend(date_files)
                print(f" {date_str}: {len(date_files)} archivos")
            except:
                print(f" {date_str}: no encontrado")
        
        print(f"\n Total archivos filtrados: {len(parquet_files)}")
        
except Exception as e:
    print(f" Error listando Bronze: {e}")
    raise

# COMMAND ----------

# Leer Bronze
df_bronze = spark.read.option("mergeSchema", "true").parquet(*parquet_files)

print(f" Leídos {df_bronze.count():,} registros de Bronze")
print("\n Schema de Bronze:")
df_bronze.printSchema()

# Mostrar muestra
display(df_bronze.show(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformaciones: Explosión y Normalización
# MAGIC
# MAGIC **Paso 1**: Explotar `raw_data.hours[]` para generar 1 fila por hora de forecast  
# MAGIC **Paso 2**: Extraer valores desde nested dicts usando `coalesce(sg, noaa, icon)`

# COMMAND ----------

# Paso 1: Explotar el array de horas
df_exploded = df_bronze.select(
    F.col("punto_id"),
    F.col("zona_id"),  # Ya viene en Bronze desde la Function
    F.col("timestamp_ingesta"),
    F.explode(F.col("raw_data.hours")).alias("hour")
)

print(f" Explosión completada: {df_exploded.count():,} registros (horas de forecast)")
print(f" Ratio explosión: {df_exploded.count() / df_bronze.count():.1f}x (aprox. horas por punto)")

# COMMAND ----------

# Paso 2: Normalizar y extraer valores desde nested dicts
df_silver = df_exploded.select(
    # claves
    F.col("punto_id").cast("string").alias("punto_id"),
    F.col("zona_id").cast("string").alias("zona_id"),
    F.to_timestamp(F.col("hour.time")).alias("timestamp_forecast"),
    F.to_timestamp(F.col("timestamp_ingesta")).alias("run_ts"),
    # oleaje
    F.col("hour.waveHeight.sg").cast("double").alias("wave_height_m"),
    F.col("hour.waveDirection.sg").cast("double").alias("wave_direction_deg"),
    F.col("hour.wavePeriod.sg").cast("double").alias("wave_period_s"),
    # swell
    F.col("hour.swellHeight.sg").cast("double").alias("swell_height_m"),
    F.col("hour.swellDirection.sg").cast("double").alias("swell_direction_deg"),
    F.col("hour.swellPeriod.sg").cast("double").alias("swell_period_s"),
    # viento
    F.col("hour.windSpeed.sg").cast("double").alias("wind_speed_ms"),
    F.col("hour.windDirection.sg").cast("double").alias("wind_direction_deg"),
    F.col("hour.gust.sg").cast("double").alias("gust_ms"),
    # atmosfera
    F.col("hour.airTemperature.sg").cast("double").alias("air_temperature_c"),
    F.col("hour.pressure.sg").cast("double").alias("pressure_hpa"),
    F.col("hour.precipitation.sg").cast("double").alias("precipitation_mm"),
    F.col("hour.cloudCover.sg").cast("double").alias("cloud_cover_pct"),
    F.col("hour.visibility.sg").cast("double").alias("visibility_km"),
    # metadata
    F.lit("stormglass").alias("source"),
    F.current_timestamp().alias("created_at")
)

print(f"Normalizacion completa: {df_silver.count():,} registros")

# Mostrar muestra
display(df_silver.show(5))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validaciones de Calidad

# COMMAND ----------

# Validación 1: Claves no nulas
null_keys = df_silver.filter(
    F.col("punto_id").isNull() | 
    F.col("timestamp_forecast").isNull() | 
    F.col("run_ts").isNull() |
    F.col("zona_id").isNull()
)

if null_keys.count() > 0:
    print(f" ERROR: {null_keys.count()} registros con claves nulas")
    display(null_keys.limit(10))
    raise Exception("Validación falló: claves nulas encontradas")
else:
    print(" Validación claves: PASS (sin nulls)")

# COMMAND ----------

# Validación 2: Rangos válidos de métricas
invalid_ranges = df_silver.filter(
    (F.col("wave_height_m") < 0) | (F.col("wave_height_m") > 20) |
    (F.col("wind_speed_ms") < 0) | (F.col("wind_speed_ms") > 50) |
    (F.col("air_temperature_c") < -20) | (F.col("air_temperature_c") > 50) |
    (F.col("pressure_hpa") < 900) | (F.col("pressure_hpa") > 1100) |
    (F.col("cloud_cover_pct") < 0) | (F.col("cloud_cover_pct") > 100) |
    (F.col("visibility_km") < 0) | (F.col("visibility_km") > 100)
)

invalid_count = invalid_ranges.count()
if invalid_count > 0:
    print(f"  {invalid_count} registros con valores fuera de rango")
    display(invalid_ranges.limit(10))
    # Filtrar registros inválidos
    df_silver = df_silver.subtract(invalid_ranges)
    print(f"Filtrados {invalid_count} registros invalidos. Quedan {df_silver.count():,} registros")
else:
    print(" Validación rangos: PASS")

# COMMAND ----------

# Validación 3: Duplicados por clave natural
dup_check = df_silver.groupBy("punto_id", "timestamp_forecast", "run_ts").count().filter(F.col("count") > 1)

if dup_check.count() > 0:
    print(f"  {dup_check.count()} grupos de duplicados encontrados")
    display(dup_check)
    # Deduplicar quedándose con el último created_at
    print("Aplicando deduplicacion...")
    from pyspark.sql.window import Window
    w = Window.partitionBy("punto_id", "timestamp_forecast", "run_ts").orderBy(F.desc("created_at"))
    df_silver = df_silver.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    print(f" Deduplicación completa: {df_silver.count():,} registros únicos")
else:
    print(" Validación duplicados: PASS")

# COMMAND ----------

# Crear vista temporal para MERGE
df_silver.createOrReplaceTempView("tmp_stormglass_new")

# Estadísticas pre-merge
print(f" Registros a mergear: {df_silver.count():,}")
print(f" Rango de run_ts: {df_silver.select(F.min('run_ts'), F.max('run_ts')).collect()[0]}")
print(f" Rango de forecast: {df_silver.select(F.min('timestamp_forecast'), F.max('timestamp_forecast')).collect()[0]}")
print(f" Puntos únicos: {df_silver.select('punto_id').distinct().count()}")
print(f" Zonas únicas: {df_silver.select('zona_id').distinct().count()}")

# Calcular ventana de forecast (horas)
forecast_window = df_silver.select(
    ((F.max("timestamp_forecast").cast("long") - F.min("run_ts").cast("long")) / 3600).alias("forecast_hours")
).collect()[0]["forecast_hours"]
print(f"  Ventana de forecast: ~{forecast_window:.0f} horas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Escribir a Silver (MERGE)
# MAGIC
# MAGIC Estrategia: UPSERT por clave primaria (punto_id, timestamp_forecast, run_ts)

# COMMAND ----------



# COMMAND ----------

# MERGE (excluyendo columna generada run_date)
spark.sql("""
MERGE INTO adb_ecoazul.silver.fact_stormglass_forecast AS target
USING tmp_stormglass_new AS source
ON target.punto_id = source.punto_id
   AND target.timestamp_forecast = source.timestamp_forecast
   AND target.run_ts = source.run_ts
WHEN MATCHED THEN UPDATE SET
  target.punto_id = source.punto_id,
  target.zona_id = source.zona_id,
  target.timestamp_forecast = source.timestamp_forecast,
  target.run_ts = source.run_ts,
  target.wave_height_m = source.wave_height_m,
  target.wave_direction_deg = source.wave_direction_deg,
  target.wave_period_s = source.wave_period_s,
  target.swell_height_m = source.swell_height_m,
  target.swell_direction_deg = source.swell_direction_deg,
  target.swell_period_s = source.swell_period_s,
  target.wind_speed_ms = source.wind_speed_ms,
  target.wind_direction_deg = source.wind_direction_deg,
  target.gust_ms = source.gust_ms,
  target.air_temperature_c = source.air_temperature_c,
  target.pressure_hpa = source.pressure_hpa,
  target.precipitation_mm = source.precipitation_mm,
  target.cloud_cover_pct = source.cloud_cover_pct,
  target.visibility_km = source.visibility_km,
  target.source = source.source,
  target.created_at = source.created_at
WHEN NOT MATCHED THEN INSERT (
  punto_id, zona_id, timestamp_forecast, run_ts,
  wave_height_m, wave_direction_deg, wave_period_s,
  swell_height_m, swell_direction_deg, swell_period_s,
  wind_speed_ms, wind_direction_deg, gust_ms,
  air_temperature_c, pressure_hpa, precipitation_mm, cloud_cover_pct, visibility_km,
  source, created_at
) VALUES (
  source.punto_id, source.zona_id, source.timestamp_forecast, source.run_ts,
  source.wave_height_m, source.wave_direction_deg, source.wave_period_s,
  source.swell_height_m, source.swell_direction_deg, source.swell_period_s,
  source.wind_speed_ms, source.wind_direction_deg, source.gust_ms,
  source.air_temperature_c, source.pressure_hpa, source.precipitation_mm, source.cloud_cover_pct, source.visibility_km,
  source.source, source.created_at
)
""")

print(" MERGE completado exitosamente")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validacion Post-Carga

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_registros,
# MAGIC   COUNT(DISTINCT zona_id) AS zonas,
# MAGIC   COUNT(DISTINCT punto_id) AS puntos,
# MAGIC   MAX(run_ts) AS ultimo_run
# MAGIC FROM adb_ecoazul.silver.fact_stormglass_forecast;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Optimización de Tabla

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize + Z-Order por columnas de filtrado frecuente
# MAGIC OPTIMIZE adb_ecoazul.silver.fact_stormglass_forecast
# MAGIC ZORDER BY (zona_id, timestamp_forecast);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Limpiar versiones antiguas (retener 7 días de historia)
# MAGIC VACUUM adb_ecoazul.silver.fact_stormglass_forecast RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Completado
# MAGIC
# MAGIC ### Resumen de Transformacion
# MAGIC - Bronze a Silver: parquets leidos y explotados por `raw_data.hours[]`
# MAGIC - Explosion: 1 registro por (punto, hora_forecast, run)
# MAGIC - Extraccion: valores desde nested dicts (sg/noaa/icon) con coalesce
# MAGIC - Normalizacion: snake_case, tipos correctos, 17 metricas oceanicas/atmosfericas
# MAGIC - MERGE: upsert por clave primaria (punto_id, timestamp_forecast, run_ts)
# MAGIC - Optimizacion: Z-Order por (zona_id, timestamp_forecast)
# MAGIC
# MAGIC ### Troubleshooting
# MAGIC - Error schema: verifica que `raw_data.hours[]` exista en Bronze
# MAGIC - Valores null masivos: verifica estructura nested dicts (sg/noaa/icon)
# MAGIC - Conteo muy bajo: ajusta `DAYS_LOOKBACK` o verifica rutas de Bronze