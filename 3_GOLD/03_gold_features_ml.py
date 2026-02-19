# Databricks notebook source
# MAGIC %md
# MAGIC ## Feature Engineering para Machine Learning
# MAGIC
# MAGIC Genera features temporales para predecir risk_score:
# MAGIC - Agregaciones por ventanas (24h, 6h)
# MAGIC - Lags temporales (1h, 3h, 6h, 12h)
# MAGIC - Features de tendencia (delta wave_height)
# MAGIC - Features de interacción (wave × wind)
# MAGIC - Features categóricas (hora del día, día de semana)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports y Configuracion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta

CATALOG = "adb_ecoazul"
GOLD = f"{CATALOG}.gold"

# Ventanas temporales para agregaciones
WINDOW_24H = 24  # Ventana de 24 horas para features agregadas
WINDOW_6H = 6    # Ventana corta para maximos/minimos
LAG_HOURS = [1, 3, 6, 12]  # Lags para series temporales

print(f"Feature Engineering Config:")
print(f"  WINDOW_24H: {WINDOW_24H}h")
print(f"  WINDOW_6H: {WINDOW_6H}h")
print(f"  LAG_HOURS: {LAG_HOURS}")
print(f"  FUENTE: gold.forecast_zona_hora (72h forecast, mismo run que scoring)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema de Tabla Gold para Features ML
# MAGIC
# MAGIC La tabla se crea/recrea en la seccion 11 antes del MERGE
# MAGIC para garantizar schema consistente.

# COMMAND ----------

print(f"Tabla destino: {GOLD}.features_ml")
print("Se creara/validara en seccion 11 antes del MERGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cargar Datos desde gold.forecast_zona_hora
# MAGIC
# MAGIC FIX DESFASE: Se lee de Gold (NO de Silver) porque:
# MAGIC - Elimina el desfase de hasta 7h en el chart V2 de PAGE 3
# MAGIC - 02_score_risk_model.py también lee de aquí → timestamps siempre sincronizados
# MAGIC - forecast_zona_hora ya integra StormGlass + Marea + Copernicus (mismo run)
# MAGIC - precipitation_3h_mm ya viene calculado en Gold

# COMMAND ----------

print(f"Fuente: gold.forecast_zona_hora")
print(f"FIX desfase: mismo run que usa 02_score_risk_model.py")

# COMMAND ----------

# FIX DESFASE TEMPORAL: leer gold.forecast_zona_hora en lugar de Silver con MAX(run_ts).
# Problema anterior: cada pipeline tomaba MAX(run_ts) de Silver en momentos distintos
# → features_ml y risk_predictions tenían runs desfasados hasta 7h en el chart V2.
# Solución: ambos notebooks leen el mismo Gold table → timestamps siempre alineados.
df_forecast = spark.table(f"{GOLD}.forecast_zona_hora")

# Cache para reusar en multiples transformaciones
df_forecast.cache()

total = df_forecast.count()
print(f"Registros en forecast_zona_hora: {total:,}")
print(f"Columnas disponibles: {len(df_forecast.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calcular Target Variable (risk_score)

# COMMAND ----------

# Calcular risk_score usando la misma formula que condiciones_actuales_zona
# Pero aplicada a TODO el forecast historico (para training)

# Calcular velocidad de corriente
df_with_current = df_forecast.withColumn(
    "corriente_speed_ms",
    F.sqrt(
        F.pow(F.coalesce(F.col("corriente_u_ms"), F.lit(0.0)), 2) +
        F.pow(F.coalesce(F.col("corriente_v_ms"), F.lit(0.0)), 2)
    )
)

# Convertir wind_speed de m/s a knots para risk_score
df_with_wind_kts = df_with_current.withColumn(
    "wind_speed_kts",
    F.col("wind_speed_ms") * 1.94384  # m/s to knots
)

# Calcular risk_score
df_with_risk = df_with_wind_kts.withColumn(
    "risk_score",
    (
        F.coalesce(F.col("wave_height_m"), F.lit(0.0)) * 10.0 +
        F.coalesce(F.col("wind_speed_kts"), F.lit(0.0)) +
        F.coalesce(F.col("corriente_speed_ms"), F.lit(0.0)) * 5.0 +
        F.when(F.coalesce(F.col("visibility_km"), F.lit(100.0)) < 2.0, 20.0).otherwise(0.0)
    )
)

# Calcular safety_level
df_with_safety = df_with_risk.withColumn(
    "safety_level",
    F.when(F.coalesce(F.col("visibility_km"), F.lit(100.0)) < 1.0, "PELIGRO")
     .when(F.col("risk_score") < 20.0, "SEGURO")
     .when(F.col("risk_score") < 40.0, "PRECAUCION")
     .otherwise("PELIGRO")
)
# FIX: umbrales corregidos de 30/60 a 20/40 para consistencia con condiciones_actuales_zona

print("Target variables calculadas: risk_score, safety_level")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Features Agregadas (Ventanas Temporales)

# COMMAND ----------

# Convertir timestamp a unix timestamp (segundos desde epoch) para rangeBetween
df_with_unix = df_with_safety.withColumn(
    "timestamp_unix",
    F.unix_timestamp("timestamp_forecast")
)

# Definir ventanas por zona, ordenadas por tiempo (usando unix timestamp)
w_24h = (
    Window
    .partitionBy("zona_id")
    .orderBy("timestamp_unix")
    .rangeBetween(-24*3600, 0)  # Ultimas 24 horas (en segundos)
)

w_6h = (
    Window
    .partitionBy("zona_id")
    .orderBy("timestamp_unix")
    .rangeBetween(-6*3600, 0)  # Ultimas 6 horas
)

# Aplicar agregaciones 24h
df_agg_24h = df_with_unix \
    .withColumn("wave_height_mean_24h", F.avg("wave_height_m").over(w_24h)) \
    .withColumn("wave_height_max_24h", F.max("wave_height_m").over(w_24h)) \
    .withColumn("wave_height_std_24h", F.stddev("wave_height_m").over(w_24h)) \
    .withColumn("wind_speed_mean_24h", F.avg("wind_speed_ms").over(w_24h)) \
    .withColumn("wind_speed_max_24h", F.max("wind_speed_ms").over(w_24h)) \
    .withColumn("precipitation_sum_24h", F.sum("precipitation_mm").over(w_24h))

# Aplicar agregaciones 6h
df_agg_6h = df_agg_24h \
    .withColumn("wave_height_max_6h", F.max("wave_height_m").over(w_6h)) \
    .withColumn("wind_speed_max_6h", F.max("wind_speed_ms").over(w_6h))

# Remover columna temporal timestamp_unix
df_agg_6h = df_agg_6h.drop("timestamp_unix")

print("Features agregadas 24h y 6h calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Features Lags Temporales

# COMMAND ----------

# Ventana para lags (ordenada, sin rango)
w_lag = Window.partitionBy("zona_id").orderBy("timestamp_forecast")

# Aplicar lags para wave_height
df_lags = df_agg_6h \
    .withColumn("wave_height_lag_1h", F.lag("wave_height_m", 1).over(w_lag)) \
    .withColumn("wave_height_lag_3h", F.lag("wave_height_m", 3).over(w_lag)) \
    .withColumn("wave_height_lag_6h", F.lag("wave_height_m", 6).over(w_lag)) \
    .withColumn("wave_height_lag_12h", F.lag("wave_height_m", 12).over(w_lag)) \
    .withColumn("wind_speed_lag_1h", F.lag("wind_speed_ms", 1).over(w_lag)) \
    .withColumn("wind_speed_lag_3h", F.lag("wind_speed_ms", 3).over(w_lag))

print("Features lags temporales calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Features de Tendencia

# COMMAND ----------

# Calcular deltas (cambio en 3 horas)
df_trends = df_lags \
    .withColumn(
        "wave_height_delta_3h",
        F.col("wave_height_m") - F.col("wave_height_lag_3h")
    ) \
    .withColumn(
        "wind_speed_delta_3h",
        F.col("wind_speed_ms") - F.col("wind_speed_lag_3h")
    )

print("Features de tendencia calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Features de Interaccion

# COMMAND ----------

df_interactions = df_trends \
    .withColumn(
        "wave_wind_interaction",
        F.col("wave_height_m") * F.col("wind_speed_ms")
    ) \
    .withColumn(
        "wave_current_interaction",
        F.col("wave_height_m") * F.col("corriente_speed_ms")
    )

print("Features de interaccion calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Features Categoricas

# COMMAND ----------

df_categorical = df_interactions \
    .withColumn("hour_of_day", F.hour("timestamp_forecast")) \
    .withColumn("day_of_week", F.dayofweek("timestamp_forecast")) \
    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin([1, 7])  # 1=Domingo, 7=Sabado
    )

print("Features categoricas calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Seleccionar Columnas Finales

# COMMAND ----------

df_features = df_categorical.select(
    # Keys
    "zona_id",
    "timestamp_forecast",

    # Target
    "risk_score",
    "safety_level",

    # Features base
    "wave_height_m",
    "wind_speed_ms",
    "air_temperature_c",
    "precipitation_mm",
    "precipitation_3h_mm",
    "visibility_km",
    "altura_marea_m",
    "temperatura_mar_c",
    "salinidad_psu",
    "corriente_speed_ms",

    # Features agregadas 24h
    "wave_height_mean_24h",
    "wave_height_max_24h",
    "wave_height_std_24h",
    "wind_speed_mean_24h",
    "wind_speed_max_24h",
    "precipitation_sum_24h",

    # Features agregadas 6h
    "wave_height_max_6h",
    "wind_speed_max_6h",

    # Lags
    "wave_height_lag_1h",
    "wave_height_lag_3h",
    "wave_height_lag_6h",
    "wave_height_lag_12h",
    "wind_speed_lag_1h",
    "wind_speed_lag_3h",

    # Tendencias
    "wave_height_delta_3h",
    "wind_speed_delta_3h",

    # Interacciones
    "wave_wind_interaction",
    "wave_current_interaction",

    # Categoricas
    "hour_of_day",
    "day_of_week",
    "is_weekend"
).withColumn(
    "created_at",
    F.current_timestamp()
)

# Filtrar filas con NULLs en target (necesario para training)
df_features_clean = df_features.where(F.col("risk_score").isNotNull())

print(f"Features finales: {len(df_features.columns)} columnas")
print(f"Registros con target valido: {df_features_clean.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Escribir a Gold

# COMMAND ----------

# CREATE IF NOT EXISTS — no borrar la tabla para acumular histórico entre runs.
# Cada ejecución agrega filas nuevas vía MERGE (sin duplicar por PK zona_id+timestamp).
# Después de 30 días de runs → ~7,200 filas de training en lugar de siempre 720.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.features_ml (
  zona_id STRING NOT NULL,
  timestamp_forecast TIMESTAMP NOT NULL,
  risk_score DOUBLE,
  safety_level STRING,
  wave_height_m DOUBLE,
  wind_speed_ms DOUBLE,
  air_temperature_c DOUBLE,
  precipitation_mm DOUBLE,
  precipitation_3h_mm DOUBLE,
  visibility_km DOUBLE,
  altura_marea_m DOUBLE,
  temperatura_mar_c DOUBLE,
  salinidad_psu DOUBLE,
  corriente_speed_ms DOUBLE,
  wave_height_mean_24h DOUBLE,
  wave_height_max_24h DOUBLE,
  wave_height_std_24h DOUBLE,
  wind_speed_mean_24h DOUBLE,
  wind_speed_max_24h DOUBLE,
  precipitation_sum_24h DOUBLE,
  wave_height_max_6h DOUBLE,
  wind_speed_max_6h DOUBLE,
  wave_height_lag_1h DOUBLE,
  wave_height_lag_3h DOUBLE,
  wave_height_lag_6h DOUBLE,
  wave_height_lag_12h DOUBLE,
  wind_speed_lag_1h DOUBLE,
  wind_speed_lag_3h DOUBLE,
  wave_height_delta_3h DOUBLE,
  wind_speed_delta_3h DOUBLE,
  wave_wind_interaction DOUBLE,
  wave_current_interaction DOUBLE,
  hour_of_day INT,
  day_of_week INT,
  is_weekend BOOLEAN,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_features_ml PRIMARY KEY (zona_id, timestamp_forecast)
) USING DELTA
COMMENT 'Features ML para prediccion de risk_score'
""")

df_features_clean.createOrReplaceTempView("tmp_features_ml")

spark.sql(f"""
MERGE INTO {GOLD}.features_ml AS t
USING tmp_features_ml AS s
ON t.zona_id = s.zona_id AND t.timestamp_forecast = s.timestamp_forecast
WHEN MATCHED THEN UPDATE SET
  t.risk_score = s.risk_score,
  t.safety_level = s.safety_level,
  t.wave_height_m = s.wave_height_m,
  t.wind_speed_ms = s.wind_speed_ms,
  t.air_temperature_c = s.air_temperature_c,
  t.precipitation_mm = s.precipitation_mm,
  t.precipitation_3h_mm = s.precipitation_3h_mm,
  t.visibility_km = s.visibility_km,
  t.altura_marea_m = s.altura_marea_m,
  t.temperatura_mar_c = s.temperatura_mar_c,
  t.salinidad_psu = s.salinidad_psu,
  t.corriente_speed_ms = s.corriente_speed_ms,
  t.wave_height_mean_24h = s.wave_height_mean_24h,
  t.wave_height_max_24h = s.wave_height_max_24h,
  t.wave_height_std_24h = s.wave_height_std_24h,
  t.wind_speed_mean_24h = s.wind_speed_mean_24h,
  t.wind_speed_max_24h = s.wind_speed_max_24h,
  t.precipitation_sum_24h = s.precipitation_sum_24h,
  t.wave_height_max_6h = s.wave_height_max_6h,
  t.wind_speed_max_6h = s.wind_speed_max_6h,
  t.wave_height_lag_1h = s.wave_height_lag_1h,
  t.wave_height_lag_3h = s.wave_height_lag_3h,
  t.wave_height_lag_6h = s.wave_height_lag_6h,
  t.wave_height_lag_12h = s.wave_height_lag_12h,
  t.wind_speed_lag_1h = s.wind_speed_lag_1h,
  t.wind_speed_lag_3h = s.wind_speed_lag_3h,
  t.wave_height_delta_3h = s.wave_height_delta_3h,
  t.wind_speed_delta_3h = s.wind_speed_delta_3h,
  t.wave_wind_interaction = s.wave_wind_interaction,
  t.wave_current_interaction = s.wave_current_interaction,
  t.hour_of_day = s.hour_of_day,
  t.day_of_week = s.day_of_week,
  t.is_weekend = s.is_weekend,
  t.created_at = s.created_at
WHEN NOT MATCHED THEN INSERT *
""")

print(f"MERGE completado: {GOLD}.features_ml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validaciones

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 1: Verificar schema
# MAGIC DESCRIBE adb_ecoazul.gold.features_ml;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 2: Conteo total
# MAGIC SELECT COUNT(*) AS total_registros
# MAGIC FROM adb_ecoazul.gold.features_ml;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 3: Distribucion de safety_level
# MAGIC SELECT
# MAGIC   safety_level,
# MAGIC   COUNT(*) AS registros,
# MAGIC   ROUND(AVG(risk_score), 2) AS avg_risk_score,
# MAGIC   ROUND(MIN(risk_score), 2) AS min_risk_score,
# MAGIC   ROUND(MAX(risk_score), 2) AS max_risk_score
# MAGIC FROM adb_ecoazul.gold.features_ml
# MAGIC GROUP BY safety_level
# MAGIC ORDER BY safety_level;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 4: NULLs en features criticas
# MAGIC SELECT
# MAGIC   COUNT(*) AS total,
# MAGIC   COUNT(risk_score) AS con_target,
# MAGIC   COUNT(wave_height_m) AS con_wave,
# MAGIC   COUNT(wave_height_lag_3h) AS con_lag_3h,
# MAGIC   COUNT(wave_height_mean_24h) AS con_mean_24h
# MAGIC FROM adb_ecoazul.gold.features_ml;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 5: Estadisticas de features numericas
# MAGIC SELECT
# MAGIC   ROUND(AVG(wave_height_m), 2) AS avg_wave,
# MAGIC   ROUND(STDDEV(wave_height_m), 2) AS std_wave,
# MAGIC   ROUND(AVG(wind_speed_ms), 2) AS avg_wind,
# MAGIC   ROUND(AVG(precipitation_sum_24h), 2) AS avg_precip_24h,
# MAGIC   ROUND(AVG(wave_height_delta_3h), 2) AS avg_wave_delta_3h
# MAGIC FROM adb_ecoazul.gold.features_ml;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 6: Ejemplo de features por zona
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   timestamp_forecast,
# MAGIC   risk_score,
# MAGIC   wave_height_m,
# MAGIC   wave_height_lag_3h,
# MAGIC   wave_height_delta_3h,
# MAGIC   hour_of_day,
# MAGIC   is_weekend
# MAGIC FROM adb_ecoazul.gold.features_ml
# MAGIC WHERE zona_id = 'BT01'
# MAGIC ORDER BY timestamp_forecast DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validacion 7: Cobertura temporal
# MAGIC SELECT
# MAGIC   MIN(timestamp_forecast) AS min_timestamp,
# MAGIC   MAX(timestamp_forecast) AS max_timestamp,
# MAGIC   DATEDIFF(MAX(timestamp_forecast), MIN(timestamp_forecast)) AS dias_cobertura
# MAGIC FROM adb_ecoazul.gold.features_ml;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Feature Summary

# COMMAND ----------

total_features_final = df_features_clean.count()

feature_summary = f"""
FEATURE ENGINEERING COMPLETADO

Fuente de datos: gold.forecast_zona_hora (72h forecast)
  - Integra StormGlass + Marea + Copernicus (mismo run)
  - Sincronizado con 02_score_risk_model.py → sin desfase

Total Features: {len(df_features.columns)}
  Target Variables: 2 (risk_score, safety_level)
  Base Features: 10
  Agregaciones 24h: 6
  Agregaciones 6h: 2
  Lags: 6 (1h, 3h, 6h, 12h)
  Tendencias: 2 (delta 3h)
  Interacciones: 2 (wave x wind, wave x current)
  Categoricas: 3 (hour, day_of_week, is_weekend)

Registros procesados: {total_features_final:,}
Horizonte: 72h (desde gold.forecast_zona_hora)

Tabla destino: {GOLD}.features_ml

SIGUIENTE PASO:
Ejecutar notebook de training: 4_ML/4_ML/01_train_risk_model.py
"""

print(feature_summary)
