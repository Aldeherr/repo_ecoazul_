# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # 05 - SILVER FACT: Marea Forecast
# MAGIC
# MAGIC ## Objetivo
# MAGIC Transformar datos crudos de Bronze (parquets de Marea API) a tablas Silver normalizadas.
# MAGIC
# MAGIC ## Estrategia de Ingesta
# MAGIC - **Frecuencia**: 6 corridas/día (02:00, 06:00, 10:00, 14:00, 18:00, 22:00 Panamá)
# MAGIC - **Cobertura**: 10 zonas × 3 puntos estratégicos = 30 puntos por corrida
# MAGIC - **Forecast**: 48 horas adelante
# MAGIC - **Granularidad**: 1 dato cada 15 minutos (192 registros por punto)
# MAGIC - **Consumo API**: 180 requests/día (27% del límite mensual)
# MAGIC
# MAGIC ## Inputs
# MAGIC - **Bronze**: `wasbs://bronze@dataecoazul/marea/forecast/`
# MAGIC - **Estructura**: Cada parquet contiene `raw_data.heights[]` (continuo) + `raw_data.extremes[]` (pleamares/bajamares)
# MAGIC
# MAGIC ## Outputs
# MAGIC - **Silver Tabla 1**: `adb_ecoazul.silver.fact_marea_forecast` (serie temporal cada 15 min)
# MAGIC - **Silver Tabla 2**: `adb_ecoazul.silver.fact_marea_extremos` (solo pleamares/bajamares)
# MAGIC
# MAGIC ## Transformaciones
# MAGIC 1. Lectura de Bronze (últimos N días)
# MAGIC 2. Explosión de `raw_data.heights[]` → serie temporal continua
# MAGIC 3. Explosión de `raw_data.extremes[]` → puntos críticos (HIGH/LOW)
# MAGIC 4. Cálculo de features derivadas (horas_hasta_cambio_marea, amplitud_dia)
# MAGIC 5. Validaciones específicas de mareas
# MAGIC 6. MERGE a 2 tablas Silver
# MAGIC
# MAGIC ## Validaciones
# MAGIC - Rangos válidos (altura 0-4m para Panamá)
# MAGIC - Frecuencia de extremos (~4 por día)
# MAGIC - Alternancia HIGH/LOW correcta
# MAGIC - Amplitud mínima (>0.2m)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adb_ecoazul;
# MAGIC USE SCHEMA silver;
# MAGIC

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime, timedelta

# Configuración de acceso a Bronze
STORAGE_ACCOUNT = "dataecoazul"
STORAGE_KEY = dbutils.secrets.get(scope="keyvault-scope", key="storage-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    STORAGE_KEY
)

BRONZE_BASE = f"wasbs://bronze@{STORAGE_ACCOUNT}.blob.core.windows.net/marea/"

print(f" Bronze path: {BRONZE_BASE}")
print(f" Configuración completada")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 2. Crear Tablas Silver (DDL)
# MAGIC
# MAGIC Se crean 2 tablas:
# MAGIC - **fact_marea_forecast**: Serie temporal continua (1 dato cada 15 min)
# MAGIC - **fact_marea_extremos**: Solo pleamares y bajamares con metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_ecoazul.silver.fact_marea_forecast (
# MAGIC   punto_id STRING NOT NULL COMMENT 'ID del punto de muestreo (ej: BT01_interior)',
# MAGIC   zona_id STRING NOT NULL COMMENT 'FK a dim_zonas',
# MAGIC   timestamp_forecast TIMESTAMP NOT NULL COMMENT 'Hora predicha (cada 15 min)',
# MAGIC   run_ts TIMESTAMP NOT NULL COMMENT 'Timestamp de la corrida que generó el forecast',
# MAGIC   
# MAGIC   -- Partición (columna generada)
# MAGIC   run_date DATE GENERATED ALWAYS AS (CAST(run_ts AS DATE)),
# MAGIC   
# MAGIC   -- Datos de marea
# MAGIC   altura_marea_m DOUBLE COMMENT 'Altura de marea en metros',
# MAGIC   
# MAGIC   -- Features derivadas (calculadas)
# MAGIC   horas_hasta_cambio_marea DOUBLE COMMENT 'Horas hasta el próximo extremo (pleamar/bajamar)',
# MAGIC   tipo_proximo_cambio STRING COMMENT 'Tipo del próximo extremo (HIGH/LOW)',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   source STRING NOT NULL COMMENT 'Fuente del dato (marea)',
# MAGIC   created_at TIMESTAMP NOT NULL COMMENT 'Timestamp de inserción en Silver',
# MAGIC   
# MAGIC   CONSTRAINT pk_marea_forecast PRIMARY KEY (punto_id, timestamp_forecast, run_ts)
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC COMMENT 'Forecast de mareas continuo (cada 15 min). 6 corridas/día × 30 puntos × 192 registros = 34,560 filas/día';

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_ecoazul.silver.fact_marea_extremos (
# MAGIC   punto_id STRING NOT NULL COMMENT 'ID del punto de muestreo',
# MAGIC   zona_id STRING NOT NULL COMMENT 'FK a dim_zonas',
# MAGIC   timestamp_extremo TIMESTAMP NOT NULL COMMENT 'Hora del extremo (pleamar/bajamar)',
# MAGIC   run_ts TIMESTAMP NOT NULL COMMENT 'Timestamp de la corrida',
# MAGIC   
# MAGIC   -- Partición (columna generada)
# MAGIC   run_date DATE GENERATED ALWAYS AS (CAST(run_ts AS DATE)),
# MAGIC   
# MAGIC   -- Datos del extremo
# MAGIC   altura_m DOUBLE NOT NULL COMMENT 'Altura del extremo en metros',
# MAGIC   tipo STRING NOT NULL COMMENT 'Tipo de extremo: HIGH (pleamar) o LOW (bajamar)',
# MAGIC   
# MAGIC   -- Features derivadas
# MAGIC   amplitud_dia_m DOUBLE COMMENT 'Rango de marea del día (max HIGH - min LOW)',
# MAGIC   calidad_estrellas INT COMMENT 'Calidad de la ventana (1-3 estrellas)',
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   source STRING NOT NULL COMMENT 'Fuente del dato (marea)',
# MAGIC   created_at TIMESTAMP NOT NULL COMMENT 'Timestamp de inserción',
# MAGIC   
# MAGIC   CONSTRAINT pk_marea_extremos PRIMARY KEY (punto_id, timestamp_extremo, run_ts)
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC COMMENT 'Extremos de marea (pleamares/bajamares). ~8 extremos × 30 puntos × 6 corridas = 1,440 filas/día';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN adb_ecoazul.silver LIKE 'fact_marea*';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de Bronze
# MAGIC
# MAGIC Estrategia: Usar función recursiva `lsR_parquet()` para navegar estructura Hive particionada.
# MAGIC
# MAGIC Estructura esperada: `wasbs://bronze@dataecoazul/marea/year=*/month=*/forecast/day=*/*.parquet`

# COMMAND ----------

def lsR_parquet(path):

    """Lista recursivamente todos los archivos .parquet en un path y subdirectorios"""
    out = []
    try:
        for fi in dbutils.fs.ls(path):
            if fi.isFile():
                if fi.path.lower().endswith(".parquet"):
                    out.append(fi.path)
            else:
                # Recursión en subdirectorios
                out.extend(lsR_parquet(fi.path))
    except Exception as e:
        print(f"Error listando {path}: {e}")
    return out  

# Parámetros de lectura
DAYS_LOOKBACK = 2  # Cambiar a None para procesar todo Bronze

# Listar TODOS los parquets recursivamente
print(" Listando archivos en Bronze (recursivo)...")
all_parquet_files = lsR_parquet(BRONZE_BASE)

if not all_parquet_files:
    raise Exception(f" No se encontraron parquets en {BRONZE_BASE}")

print(f" Encontrados {len(all_parquet_files)} archivos parquet en Bronze")

# Filtrar por fecha si es incremental
if DAYS_LOOKBACK:
    from datetime import datetime, timedelta
    
    # Generar fechas de los últimos N días
    valid_dates = [
        (datetime.utcnow() - timedelta(days=i)).strftime("%Y%m%d")
        for i in range(DAYS_LOOKBACK + 1)
    ]
    
    # Filtrar archivos que contengan alguna de las fechas válidas
    parquet_files = [
        p for p in all_parquet_files 
        if any(date_str in p for date_str in valid_dates)
    ]
    
    print(f" Filtrando últimos {DAYS_LOOKBACK} días: {len(parquet_files)} archivos")
    
    if not parquet_files:
        print(f"  No se encontraron archivos de los últimos {DAYS_LOOKBACK} días")
        print(f"  Procesando TODOS los archivos en su lugar...")
        parquet_files = all_parquet_files
else:
    parquet_files = all_parquet_files

# Mostrar muestra de archivos
print(f"\n Total archivos a procesar: {len(parquet_files)}")
print("\n Muestra de archivos (últimos 5):")
for f in sorted(parquet_files)[-5:]:
    print(f"   {f.split('/')[-1]}")

# COMMAND ----------

# Leer Bronze
print(f" Leyendo {len(parquet_files)} archivos parquet...")
df_bronze = spark.read.parquet(*parquet_files)

print(f" Leídos {df_bronze.count():,} registros de Bronze")
print("\n Schema de Bronze:")
df_bronze.printSchema()

# Mostrar muestra
display(df_bronze.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Verificación: Inspeccionar estructura de raw_data
# MAGIC
# MAGIC Verificamos que existan los arrays `heights[]` y `extremes[]`

# COMMAND ----------


# Ver estructura de raw_data
sample = df_bronze.select("raw_data").first()

if sample and sample['raw_data']:
    raw_data = sample['raw_data']
    
    print(" Estructura de raw_data:")
    print(f"   ├─ heights: {len(raw_data['heights']) if raw_data['heights'] else 0} ")
    print(f"   └─ extremes: {len(raw_data['extremes']) if raw_data['extremes'] else 0}")
    
    if raw_data['heights']:
        print("\n Ejemplo heights[0]:")
        print(raw_data['heights'][0])
    
    if raw_data['extremes']:
        print("\n Ejemplo extremes[0]:")
        print(raw_data['extremes'][0])
else:
    print("  No se pudo leer raw_data. Verifica estructura de Bronze.")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 4. Transformación: Explosión de heights[]
# MAGIC
# MAGIC Explotar `raw_data.heights[]` para generar serie temporal (1 fila por hora de forecast)

# COMMAND ----------

# Paso 1: Explotar el array de heights
df_heights_exploded = df_bronze.select(
    F.col("punto_id"),
    F.col("zona_id"),
    F.col("timestamp_ingesta"),
    F.explode(F.col("raw_data.heights")).alias("height_record")
)

print(f" Explosión heights: {df_heights_exploded.count():,} registros")
print(f" Ratio explosión: {df_heights_exploded.count() / df_bronze.count():.1f}x (horas de forecast por punto)")

# Mostrar muestra
display(df_heights_exploded.limit(3))

# COMMAND ----------

# COMMAND ----------

# Paso 2: Normalizar columnas de heights
df_heights_normalized = df_heights_exploded.select(
    # Claves
    F.col("punto_id").cast("string").alias("punto_id"),
    F.col("zona_id").cast("string").alias("zona_id"),
    F.to_timestamp(F.col("height_record.datetime")).alias("timestamp_forecast"),
    F.to_timestamp(F.col("timestamp_ingesta")).alias("run_ts"),
    
    # Datos de marea
    F.col("height_record.height").cast("double").alias("altura_marea_m"),
    F.col("height_record.state").cast("string").alias("estado_marea"),  # RISING/FALLING
    
    # Metadata
    F.lit("marea").alias("source"),
    F.current_timestamp().alias("created_at")
)

print(f" Normalización heights: {df_heights_normalized.count():,} registros")

# Mostrar muestra
display(df_heights_normalized.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transformación: Explosión de extremes[]
# MAGIC
# MAGIC Explotar `raw_data.extremes[]` para generar tabla de pleamares/bajamares (puntos críticos)

# COMMAND ----------


# Paso 1: Explotar el array de extremes
df_extremes_exploded = df_bronze.select(
    F.col("punto_id"),
    F.col("zona_id"),
    F.col("timestamp_ingesta"),
    F.explode(F.col("raw_data.extremes")).alias("extreme_record")
)

print(f" Explosión extremes completada: {df_extremes_exploded.count():,} registros")
print(f" Ratio explosión: {df_extremes_exploded.count() / df_bronze.count():.1f}x (extremos por punto)")

# Mostrar muestra
display(df_extremes_exploded.limit(3))

# COMMAND ----------

# COMMAND ----------

# Paso 2: Normalizar columnas de extremes
df_extremes_normalized = df_extremes_exploded.select(
    # Claves
    F.col("punto_id").cast("string").alias("punto_id"),
    F.col("zona_id").cast("string").alias("zona_id"),
    F.to_timestamp(F.col("extreme_record.datetime")).alias("timestamp_extremo"),
    F.to_timestamp(F.col("timestamp_ingesta")).alias("run_ts"),
    
    # Datos del extremo
    F.col("extreme_record.height").cast("double").alias("altura_m"),
    
    # Mapear state: "HIGH_TIDE" → "HIGH", "LOW_TIDE" → "LOW"
    F.when(F.col("extreme_record.state") == "HIGH_TIDE", "HIGH")
     .when(F.col("extreme_record.state") == "LOW_TIDE", "LOW")
     .otherwise(F.col("extreme_record.state"))
     .alias("tipo"),
    
    # Metadata
    F.lit("marea").alias("source"),
    F.current_timestamp().alias("created_at")
)

print(f" Normalización extremes: {df_extremes_normalized.count():,} registros")

# Verificar distribución de tipos
print("\n Distribución de tipos (HIGH/LOW):")
df_extremes_normalized.groupBy("tipo").count().show()

# Mostrar muestra
display(df_extremes_normalized.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Calcular Features Derivadas
# MAGIC
# MAGIC ### Feature Crítica: `horas_hasta_cambio_marea`
# MAGIC
# MAGIC Por cada registro de `heights` (serie temporal), calcular:
# MAGIC - Cuántas horas faltan hasta el próximo extremo (pleamar/bajamar)
# MAGIC - Qué tipo de extremo es el próximo (HIGH/LOW)
# MAGIC
# MAGIC **Técnica**: Cross join + filtrado + window function para obtener el extremo MÁS CERCANO futuro.

# COMMAND ----------

# Paso 1: Cross join heights con extremes del mismo punto/corrida
# Solo tomamos extremos FUTUROS (timestamp_extremo > timestamp_forecast)

df_with_next_extreme = df_heights_normalized.alias("h").join(
    df_extremes_normalized.select(
        F.col("punto_id").alias("e_punto_id"),
        F.col("run_ts").alias("e_run_ts"),
        F.col("timestamp_extremo"),
        F.col("tipo").alias("tipo_extremo")
    ).alias("e"),
    on=[
        F.col("h.punto_id") == F.col("e.e_punto_id"),
        F.col("h.run_ts") == F.col("e.e_run_ts"),
        F.col("e.timestamp_extremo") > F.col("h.timestamp_forecast")  # Solo futuros
    ],
    how="left"  # Left join para mantener todos los heights
)

print(f" Join heights + extremes: {df_with_next_extreme.count():,} registros")
print(" Este count puede ser alto (varios extremos por cada height)")

# COMMAND ----------

# COMMAND ----------

# Paso 2: Por cada height, quedarse SOLO con el extremo más cercano
from pyspark.sql.window import Window

# Window: particionar por (punto, timestamp_forecast, run_ts), ordenar por timestamp_extremo ASC
w = Window.partitionBy("punto_id", "timestamp_forecast", "run_ts").orderBy("timestamp_extremo")

df_with_closest_extreme = df_with_next_extreme.withColumn(
    "rn", F.row_number().over(w)
).filter(
    F.col("rn") == 1  # Solo el extremo más cercano
).drop("rn", "e_punto_id", "e_run_ts")

print(f"Filtrado al extremo mas cercano: {df_with_closest_extreme.count():,} registros")
print("Debe ser igual a df_heights_normalized")

# Paso 3: Calcular horas hasta el cambio
df_heights_with_features = df_with_closest_extreme.withColumn(
    "horas_hasta_cambio_marea",
    (F.unix_timestamp("timestamp_extremo") - F.unix_timestamp("timestamp_forecast")) / 3600.0
).withColumn(
    "tipo_proximo_cambio",
    F.col("tipo_extremo")
).drop("timestamp_extremo", "tipo_extremo")

print(f"Features derivadas calculadas: {df_heights_with_features.count():,} registros")

# Verificar rangos
print("\nEstadisticas de horas_hasta_cambio_marea:")
df_heights_with_features.select(
    F.min("horas_hasta_cambio_marea").alias("min_horas"),
    F.avg("horas_hasta_cambio_marea").alias("avg_horas"),
    F.max("horas_hasta_cambio_marea").alias("max_horas")
).show()

# Mostrar muestra
display(df_heights_with_features.limit(5))

# COMMAND ----------

# COMMAND ----------

# Calcular amplitud del día usando los tipos REALES (HIGH_TIDE/LOW_TIDE)

# Window por punto, run_ts, y día
w_dia = Window.partitionBy(
    "punto_id", 
    "run_ts", 
    F.to_date("timestamp_extremo")
)

df_extremes_with_amplitud = df_extremes_normalized.withColumn(
    # Max HIGH del día
    "max_high_dia", 
    F.max(
        F.when(
            F.col("tipo").contains("HIGH"),  # ← Captura HIGH_TIDE o HIGH
            F.col("altura_m")
        )
    ).over(w_dia)
).withColumn(
    # Min LOW del día
    "min_low_dia",
    F.min(
        F.when(
            F.col("tipo").contains("LOW"),   # ← Captura LOW_TIDE o LOW
            F.col("altura_m")
        )
    ).over(w_dia)
).withColumn(
    # Amplitud = diferencia absoluta
    "amplitud_dia_m",
    F.when(
        F.col("max_high_dia").isNotNull() & F.col("min_low_dia").isNotNull(),
        F.abs(F.col("max_high_dia") - F.col("min_low_dia"))
    ).otherwise(F.lit(None))
).withColumn(
    # Calidad en estrellas - ajustado para Caribe (amplitudes pequeñas)
    "calidad_estrellas",
    F.when(F.col("amplitud_dia_m").isNull(), 1)
     .when(F.col("amplitud_dia_m") >= 2.0, 3)      # Marea muy grande
     .when(F.col("amplitud_dia_m") >= 1.0, 2)      # Marea grande
     .when(F.col("amplitud_dia_m") >= 0.5, 2)      # Marea media (común en Caribe)
     .otherwise(1)                                   # Marea pequeña
).drop("max_high_dia", "min_low_dia")

print(f"Amplitud calculada: {df_extremes_with_amplitud.count():,} registros")

# Estadísticas de amplitud
print("\n Estadísticas de amplitud_dia_m:")
df_extremes_with_amplitud.select(
    F.count("*").alias("total"),
    F.count("amplitud_dia_m").alias("non_null"),
    F.min("amplitud_dia_m").alias("min_amp"),
    F.avg("amplitud_dia_m").alias("avg_amp"),
    F.max("amplitud_dia_m").alias("max_amp")
).show()

# Distribución de calidad
print("\n Distribución de calidad (estrellas):")
df_extremes_with_amplitud.groupBy("calidad_estrellas").count().orderBy("calidad_estrellas").show()

# Mostrar muestra con amplitud visible
display(df_extremes_with_amplitud.select(
    "punto_id", "zona_id", "timestamp_extremo", "altura_m", "tipo", 
    "amplitud_dia_m", "calidad_estrellas"
).limit(10))

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## 7. Validaciones de Calidad
# MAGIC
# MAGIC Validaciones específicas para mareas:
# MAGIC 1. Claves no nulas
# MAGIC 2. Rangos válidos de altura (0-4m típico de Panamá)
# MAGIC 3. Valores razonables de horas_hasta_cambio_marea
# MAGIC 4. Sin duplicados por clave primaria

# COMMAND ----------

# COMMAND ----------

# Validación 1: Claves no nulas en heights
null_keys_heights = df_heights_with_features.filter(
    F.col("punto_id").isNull() | 
    F.col("zona_id").isNull() |
    F.col("timestamp_forecast").isNull() | 
    F.col("run_ts").isNull()
)

if null_keys_heights.count() > 0:
    print(f" ERROR: {null_keys_heights.count()} registros heights con claves nulas")
    display(null_keys_heights.limit(10))
    raise Exception("Validación falló: claves nulas en heights")
else:
    print(" Validación heights - Claves: PASS (sin nulls)")

# COMMAND ----------

# Validación 2: Rangos válidos en heights
invalid_heights = df_heights_with_features.filter(
    (F.col("altura_marea_m") < -2.0) | (F.col("altura_marea_m") > 6.0) |  # Rango amplio Panamá
    (F.col("horas_hasta_cambio_marea") < 0) | (F.col("horas_hasta_cambio_marea") > 12)  # Max 12h entre extremos
)

invalid_count = invalid_heights.count()
if invalid_count > 0:
    print(f"  {invalid_count} registros con valores fuera de rango")
    display(invalid_heights.limit(10))
    # Filtrar registros inválidos
    df_heights_with_features = df_heights_with_features.subtract(invalid_heights)
    print(f" Filtrados {invalid_count} registros inválidos. Quedan {df_heights_with_features.count():,}")
else:
    print(" Validación heights - Rangos: PASS")

# COMMAND ----------

# Validación 3: Duplicados por clave en heights
dup_check_heights = df_heights_with_features.groupBy(
    "punto_id", "timestamp_forecast", "run_ts"
).count().filter(F.col("count") > 1)

if dup_check_heights.count() > 0:
    print(f"    {dup_check_heights.count()} grupos de duplicados encontrados en heights")
    display(dup_check_heights)
    # Deduplicar
    print(" Aplicando deduplicación...")
    from pyspark.sql.window import Window
    w = Window.partitionBy("punto_id", "timestamp_forecast", "run_ts").orderBy(F.desc("created_at"))
    df_heights_with_features = df_heights_with_features.withColumn(
        "rn", F.row_number().over(w)
    ).filter(F.col("rn") == 1).drop("rn")
    print(f" Deduplicación completa: {df_heights_with_features.count():,} registros únicos")
else:
    print(" Validación heights - Duplicados: PASS")

# COMMAND ----------

# COMMAND ----------

# Validación 4: Claves no nulas en extremos
null_keys_extremes = df_extremes_with_amplitud.filter(
    F.col("punto_id").isNull() | 
    F.col("zona_id").isNull() |
    F.col("timestamp_extremo").isNull() | 
    F.col("run_ts").isNull() |
    F.col("altura_m").isNull() |
    F.col("tipo").isNull()
)

if null_keys_extremes.count() > 0:
    print(f" ERROR: {null_keys_extremes.count()} registros extremos con claves nulas")
    display(null_keys_extremes.limit(10))
    raise Exception("Validación falló: claves nulas en extremos")
else:
    print(" Validación extremos - Claves: PASS")

# Validación 5: Duplicados por clave en extremos
dup_check_extremes = df_extremes_with_amplitud.groupBy(
    "punto_id", "timestamp_extremo", "run_ts"
).count().filter(F.col("count") > 1)

if dup_check_extremes.count() > 0:
    print(f"  {dup_check_extremes.count()} grupos de duplicados en extremos")
    display(dup_check_extremes)
    # Deduplicar
    w = Window.partitionBy("punto_id", "timestamp_extremo", "run_ts").orderBy(F.desc("created_at"))
    df_extremes_with_amplitud = df_extremes_with_amplitud.withColumn(
        "rn", F.row_number().over(w)
    ).filter(F.col("rn") == 1).drop("rn")
    print(f" Deduplicación extremos: {df_extremes_with_amplitud.count():,} registros")
else:
    print(" Validación extremos - Duplicados: PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Escribir a Silver (MERGE)
# MAGIC
# MAGIC Estrategia: UPSERT por clave primaria en ambas tablas:
# MAGIC - **fact_marea_forecast**: (punto_id, timestamp_forecast, run_ts)
# MAGIC - **fact_marea_extremos**: (punto_id, timestamp_extremo, run_ts)

# COMMAND ----------


# Preparar df_heights_with_features para MERGE
# Importante: Excluir run_date (columna generada)

df_forecast_final = df_heights_with_features.select(
    "punto_id",
    "zona_id",
    "timestamp_forecast",
    "run_ts",
    "altura_marea_m",
    "horas_hasta_cambio_marea",
    "tipo_proximo_cambio",
    "source",
    "created_at"
)

# Crear vista temporal
df_forecast_final.createOrReplaceTempView("tmp_marea_forecast_new")

# Estadísticas pre-merge
print(f"Registros a mergear (forecast): {df_forecast_final.count():,}")
print(f"Rango run_ts: {df_forecast_final.select(F.min('run_ts'), F.max('run_ts')).collect()[0]}")
print(f"Rango forecast: {df_forecast_final.select(F.min('timestamp_forecast'), F.max('timestamp_forecast')).collect()[0]}")
print(f"Puntos unicos: {df_forecast_final.select('punto_id').distinct().count()}")
print(f"Zonas unicas: {df_forecast_final.select('zona_id').distinct().count()}")

# COMMAND ----------

# COMMAND ----------

# MERGE fact_marea_forecast
spark.sql("""
MERGE INTO adb_ecoazul.silver.fact_marea_forecast AS target
USING tmp_marea_forecast_new AS source
ON target.punto_id = source.punto_id 
   AND target.timestamp_forecast = source.timestamp_forecast
   AND target.run_ts = source.run_ts
WHEN MATCHED THEN UPDATE SET 
  target.punto_id = source.punto_id,
  target.zona_id = source.zona_id,
  target.timestamp_forecast = source.timestamp_forecast,
  target.run_ts = source.run_ts,
  target.altura_marea_m = source.altura_marea_m,
  target.horas_hasta_cambio_marea = source.horas_hasta_cambio_marea,
  target.tipo_proximo_cambio = source.tipo_proximo_cambio,
  target.source = source.source,
  target.created_at = source.created_at
WHEN NOT MATCHED THEN INSERT (
  punto_id, zona_id, timestamp_forecast, run_ts,
  altura_marea_m, horas_hasta_cambio_marea, tipo_proximo_cambio,
  source, created_at
) VALUES (
  source.punto_id, source.zona_id, source.timestamp_forecast, source.run_ts,
  source.altura_marea_m, source.horas_hasta_cambio_marea, source.tipo_proximo_cambio,
  source.source, source.created_at
)
""")

print("MERGE fact_marea_forecast completado")

# COMMAND ----------


# Preparar df_extremes_with_amplitud para MERGE
# Excluir run_date (columna generada)
df_extremos_final = df_extremes_with_amplitud.select(
    "punto_id",
    "zona_id",
    "timestamp_extremo",
    "run_ts",
    "altura_m",
    "tipo",
    "amplitud_dia_m",
    "calidad_estrellas",
    "source",
    "created_at"
)

# Crear vista temporal
df_extremos_final.createOrReplaceTempView("tmp_marea_extremos_new")

# Estadísticas pre-merge
print(f" Registros a mergear (extremos): {df_extremos_final.count():,}")
print(f" Rango extremos: {df_extremos_final.select(F.min('timestamp_extremo'), F.max('timestamp_extremo')).collect()[0]}")
print(f" Puntos únicos: {df_extremos_final.select('punto_id').distinct().count()}")
print(f"\n Distribución tipos: {df_extremos_final.groupBy("tipo").count().show()}")

# COMMAND ----------

# MERGE fact_marea_extremos
spark.sql("""
          MERGE INTO adb_ecoazul.silver.fact_marea_extremos AS target
          USING tmp_marea_extremos_new AS source 
          ON target.punto_id = source.punto_id 
          AND target.timestamp_extremo = source.timestamp_extremo
          AND target.run_ts = source.run_ts
          WHEN MATCHED THEN UPDATE SET 
            target.punto_id = source.punto_id,
            target.zona_id = source.zona_id,
            target.timestamp_extremo = source.timestamp_extremo,
            target.run_ts = source.run_ts,
            target.altura_m = source.altura_m,
            target.tipo = source.tipo,
            target.amplitud_dia_m = source.amplitud_dia_m,
            target.calidad_estrellas = source.calidad_estrellas,
            target.source = source.source,
            target.created_at = source.created_at
            WHEN NOT MATCHED THEN INSERT (
                punto_id, zona_id, timestamp_extremo, run_ts,
                altura_m, tipo, amplitud_dia_m, calidad_estrellas,
                source, created_at) 
                VALUES (
                    source.punto_id, source.zona_id, source.timestamp_extremo, source.run_ts,
                    source.altura_m, source.tipo, source.amplitud_dia_m, source.calidad_estrellas,
                    source.source, source.created_at)
""")

print("MERGE fact_marea_extremos completado")

# Verificar resultado
count_final = spark.sql("SELECT COUNT(*) FROM adb_ecoazul.silver.fact_marea_extremos").collect()[0][0]
print(f"Tabla fact_marea_extremos ahora tiene: {count_final:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validacion Post-Carga

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'fact_marea_forecast' AS tabla,
# MAGIC   COUNT(*) AS total_registros,
# MAGIC   COUNT(DISTINCT zona_id) AS zonas,
# MAGIC   MAX(run_ts) AS ultimo_run
# MAGIC FROM adb_ecoazul.silver.fact_marea_forecast
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'fact_marea_extremos' AS tabla,
# MAGIC   COUNT(*) AS total_registros,
# MAGIC   COUNT(DISTINCT zona_id) AS zonas,
# MAGIC   MAX(run_ts) AS ultimo_run
# MAGIC FROM adb_ecoazul.silver.fact_marea_extremos;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC %md
# MAGIC ## 10. Optimización de Tablas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize + Z-Order fact_marea_forecast
# MAGIC OPTIMIZE adb_ecoazul.silver.fact_marea_forecast
# MAGIC ZORDER BY (zona_id, timestamp_forecast);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize + Z-Order fact_marea_extremos
# MAGIC OPTIMIZE adb_ecoazul.silver.fact_marea_extremos
# MAGIC ZORDER BY (zona_id, timestamp_extremo);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vacuum ambas tablas (limpiar versiones antiguas >7 días)
# MAGIC VACUUM adb_ecoazul.silver.fact_marea_forecast RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC VACUUM adb_ecoazul.silver.fact_marea_extremos RETAIN 168 HOURS;

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Notebook Completado
# MAGIC
# MAGIC ### Resumen
# MAGIC - Bronze a Silver: 2 tablas Delta creadas y pobladas
# MAGIC - fact_marea_forecast: serie temporal cada hora por punto
# MAGIC - fact_marea_extremos: pleamares y bajamares con amplitud y calidad
# MAGIC - Features derivadas: horas_hasta_cambio_marea, amplitud_dia_m
# MAGIC - Optimizacion: Z-Order + Vacuum aplicado