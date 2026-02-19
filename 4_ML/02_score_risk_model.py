# Databricks notebook source
# MAGIC %md
# MAGIC ## Scoring Pipeline: Inferencia de Risk Score
# MAGIC
# MAGIC Carga el modelo entrenado desde MLflow y genera predicciones
# MAGIC para las próximas 72 horas usando gold.forecast_zona_hora.
# MAGIC
# MAGIC Output: gold.risk_predictions (zona_id × timestamp_forecast)
# MAGIC
# MAGIC Ejecutar DESPUES de que gold.forecast_zona_hora esté actualizado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports y Configuracion

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from sklearn.preprocessing import LabelEncoder
from datetime import datetime

CATALOG  = "adb_ecoazul"
GOLD     = f"{CATALOG}.gold"

# Nombre del modelo registrado en MLflow Model Registry
MODEL_NAME    = "risk_score_predictor"
MODEL_STAGE   = "latest"   # "latest" | "Production" | "Staging"

# Version del modelo (para trazabilidad en risk_predictions)
MODEL_VERSION = "GBT_v1"

current_user = spark.sql("SELECT current_user()").collect()[0][0]
EXPERIMENT_NAME = f"/Users/{current_user}/risk_score_prediction"

print(f"Scoring Configuration:")
print(f"  MODEL_NAME:  {MODEL_NAME}")
print(f"  MODEL_STAGE: {MODEL_STAGE}")
print(f"  Fuente:      {GOLD}.forecast_zona_hora")
print(f"  Destino:     {GOLD}.risk_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cargar Modelo desde MLflow

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_NAME)

try:
    # Intentar cargar desde Model Registry
    model_uri = f"models:/{MODEL_NAME}/{MODEL_STAGE}"
    model = mlflow.sklearn.load_model(model_uri)
    run_id = "registry"
    print(f"Modelo cargado desde Registry: {model_uri}")
except Exception:
    # Fallback: cargar el último run del experiment
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=1
    )
    if runs.empty:
        raise Exception("No hay runs en MLflow. Ejecuta primero 01_train_risk_model.py")
    run_id = runs.iloc[0]["run_id"]
    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)
    print(f"Modelo cargado desde último run: {run_id}")

print(f"Tipo de modelo: {type(model).__name__}")
print(f"Features esperadas: {len(model.feature_names_in_) if hasattr(model, 'feature_names_in_') else 'N/A'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cargar forecast_zona_hora (próximas 72h)

# COMMAND ----------

df_fg = spark.table(f"{GOLD}.forecast_zona_hora")

total = df_fg.count()
print(f"Registros en forecast_zona_hora: {total:,}")
print(f"Columnas disponibles: {len(df_fg.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Calcular Features Base

# COMMAND ----------

# Corriente escalar (no existe en forecast_zona_hora, calcular desde u y v)
df_base = df_fg.withColumn(
    "corriente_speed_ms",
    F.sqrt(
        F.pow(F.coalesce(F.col("corriente_u_ms"), F.lit(0.0)), 2) +
        F.pow(F.coalesce(F.col("corriente_v_ms"), F.lit(0.0)), 2)
    )
).withColumn(
    "wind_speed_kts",
    F.col("wind_speed_ms") * 1.94384
)

# Features categoricas temporales
df_base = df_base \
    .withColumn("hour_of_day",  F.hour("timestamp_forecast")) \
    .withColumn("day_of_week",  F.dayofweek("timestamp_forecast")) \
    .withColumn("is_weekend",   F.col("day_of_week").isin([1, 7]))

print("Features base calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Calcular Lags y Agregaciones (dentro de la ventana de 72h)
# MAGIC
# MAGIC NOTA: Los lags se calculan dentro del forecast (no desde Silver histórico).
# MAGIC Las primeras horas de cada zona tendrán NULLs en lags → se rellenan con 0.
# MAGIC Esto es aceptable para inferencia sobre datos futuros.

# COMMAND ----------

w_lag  = Window.partitionBy("zona_id").orderBy("timestamp_forecast")
w_unix = Window.partitionBy("zona_id").orderBy(F.unix_timestamp("timestamp_forecast"))
w_24h  = w_unix.rangeBetween(-24*3600, 0)
w_6h   = w_unix.rangeBetween(-6*3600, 0)

df_feat = df_base \
    .withColumn("timestamp_unix", F.unix_timestamp("timestamp_forecast")) \
    .withColumn("wave_height_lag_1h",    F.lag("wave_height_m",  1).over(w_lag)) \
    .withColumn("wave_height_lag_3h",    F.lag("wave_height_m",  3).over(w_lag)) \
    .withColumn("wave_height_lag_6h",    F.lag("wave_height_m",  6).over(w_lag)) \
    .withColumn("wave_height_lag_12h",   F.lag("wave_height_m", 12).over(w_lag)) \
    .withColumn("wind_speed_lag_1h",     F.lag("wind_speed_ms",  1).over(w_lag)) \
    .withColumn("wind_speed_lag_3h",     F.lag("wind_speed_ms",  3).over(w_lag)) \
    .withColumn("wave_height_delta_3h",
        F.col("wave_height_m") - F.lag("wave_height_m", 3).over(w_lag)) \
    .withColumn("wind_speed_delta_3h",
        F.col("wind_speed_ms") - F.lag("wind_speed_ms", 3).over(w_lag)) \
    .withColumn("wave_height_mean_24h",  F.avg("wave_height_m").over(w_24h)) \
    .withColumn("wave_height_max_24h",   F.max("wave_height_m").over(w_24h)) \
    .withColumn("wave_height_std_24h",   F.stddev("wave_height_m").over(w_24h)) \
    .withColumn("wind_speed_mean_24h",   F.avg("wind_speed_ms").over(w_24h)) \
    .withColumn("wind_speed_max_24h",    F.max("wind_speed_ms").over(w_24h)) \
    .withColumn("precipitation_sum_24h", F.sum(
        F.coalesce(F.col("precipitation_mm"), F.lit(0.0))).over(w_24h)) \
    .withColumn("wave_height_max_6h",    F.max("wave_height_m").over(w_6h)) \
    .withColumn("wind_speed_max_6h",     F.max("wind_speed_ms").over(w_6h)) \
    .withColumn("wave_wind_interaction",
        F.col("wave_height_m") * F.col("wind_speed_ms")) \
    .withColumn("wave_current_interaction",
        F.col("wave_height_m") * F.col("corriente_speed_ms")) \
    .drop("timestamp_unix")

print("Lags y agregaciones calculadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preparar Vector de Features para el Modelo

# COMMAND ----------

# Mismo orden que en training (critico para scikit-learn)
numeric_features = [
    "wave_height_m", "wind_speed_ms", "air_temperature_c",
    "precipitation_mm", "precipitation_3h_mm", "visibility_km",
    "altura_marea_m", "temperatura_mar_c", "salinidad_psu",
    "corriente_speed_ms",
    "wave_height_mean_24h", "wave_height_max_24h", "wave_height_std_24h",
    "wind_speed_mean_24h", "wind_speed_max_24h", "precipitation_sum_24h",
    "wave_height_max_6h", "wind_speed_max_6h",
    "wave_height_lag_1h", "wave_height_lag_3h", "wave_height_lag_6h",
    "wave_height_lag_12h", "wind_speed_lag_1h", "wind_speed_lag_3h",
    "wave_height_delta_3h", "wind_speed_delta_3h",
    # wave_wind_interaction y wave_current_interaction excluidos (correlación ~0.96 con target)
    # Debe coincidir exactamente con numeric_features de 01_train_risk_model.py
    "hour_of_day", "day_of_week"
]

# Convertir a Pandas para scoring
cols_needed = ["zona_id", "timestamp_forecast"] + numeric_features
pdf_score = df_feat.select(cols_needed).toPandas()

print(f"Registros a puntuar: {len(pdf_score):,}")

# Encoding de zona_id (mismo LabelEncoder que training — replicar mapeo)
le = LabelEncoder()
le.fit(pdf_score["zona_id"].astype(str))
pdf_score["zona_id_encoded"] = le.transform(pdf_score["zona_id"].astype(str))

# Rellenar NULLs con 0 (lags de primeras horas del forecast)
feature_cols = numeric_features + ["zona_id_encoded"]
X_score = pdf_score[feature_cols].fillna(0)

print(f"Features para modelo: {X_score.shape[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Generar Predicciones

# COMMAND ----------

y_pred = model.predict(X_score)

# Aplicar umbrales de safety_level (20/40 — consistente con todo el sistema)
def risk_to_safety(score):
    """Convierte un risk_score numerico en nivel de seguridad textual.

    Umbrales consistentes con gold.condiciones_actuales_zona y gold.features_ml:
    - score >= 40  → PELIGRO
    - score >= 20  → PRECAUCION
    - score < 20   → SEGURO

    Args:
        score: Valor numerico del risk_score predicho (rango esperado 0-100).

    Returns:
        String con el nivel: 'PELIGRO', 'PRECAUCION' o 'SEGURO'.
    """
    if score >= 40:
        return "PELIGRO"
    elif score >= 20:
        return "PRECAUCION"
    else:
        return "SEGURO"

pdf_score["risk_score_pred"]   = np.clip(y_pred, 0, 100).round(2)
pdf_score["safety_level_pred"] = pdf_score["risk_score_pred"].apply(risk_to_safety)
pdf_score["model_version"]     = MODEL_VERSION
pdf_score["model_run_id"]      = run_id
pdf_score["prediction_ts"]     = datetime.now()
pdf_score["created_at"]        = datetime.now()

print(f"Predicciones generadas: {len(pdf_score):,}")
print(f"Distribución safety_level_pred:")
print(pdf_score["safety_level_pred"].value_counts().to_string())
print(f"\nRisk score pred — stats:")
print(pdf_score["risk_score_pred"].describe().round(2).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Guardar en gold.risk_predictions

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.risk_predictions (
  zona_id          STRING    NOT NULL,
  timestamp_forecast TIMESTAMP NOT NULL,
  risk_score_pred  DOUBLE    NOT NULL,
  safety_level_pred STRING   NOT NULL,
  model_version    STRING    NOT NULL,
  model_run_id     STRING    NOT NULL,
  prediction_ts    TIMESTAMP NOT NULL,
  created_at       TIMESTAMP NOT NULL,
  CONSTRAINT pk_risk_predictions PRIMARY KEY (zona_id, timestamp_forecast)
) USING DELTA
COMMENT 'Predicciones ML de risk_score para las próximas 72h por zona'
""")

output_cols = [
    "zona_id", "timestamp_forecast",
    "risk_score_pred", "safety_level_pred",
    "model_version", "model_run_id",
    "prediction_ts", "created_at"
]

df_out = spark.createDataFrame(pdf_score[output_cols])
df_out.createOrReplaceTempView("tmp_risk_predictions")

spark.sql(f"""
MERGE INTO {GOLD}.risk_predictions AS t
USING tmp_risk_predictions AS s
ON t.zona_id = s.zona_id AND t.timestamp_forecast = s.timestamp_forecast
WHEN MATCHED THEN UPDATE SET
  t.risk_score_pred   = s.risk_score_pred,
  t.safety_level_pred = s.safety_level_pred,
  t.model_version     = s.model_version,
  t.model_run_id      = s.model_run_id,
  t.prediction_ts     = s.prediction_ts,
  t.created_at        = s.created_at
WHEN NOT MATCHED THEN INSERT *
""")

print(f"MERGE completado: {GOLD}.risk_predictions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validacion

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   COUNT(*) AS horas_predichas,
# MAGIC   ROUND(AVG(risk_score_pred), 2) AS avg_risk_pred,
# MAGIC   ROUND(MIN(risk_score_pred), 2) AS min_risk_pred,
# MAGIC   ROUND(MAX(risk_score_pred), 2) AS max_risk_pred,
# MAGIC   COUNT(CASE WHEN safety_level_pred = 'SEGURO'    THEN 1 END) AS horas_seguro,
# MAGIC   COUNT(CASE WHEN safety_level_pred = 'PRECAUCION' THEN 1 END) AS horas_precaucion,
# MAGIC   COUNT(CASE WHEN safety_level_pred = 'PELIGRO'   THEN 1 END) AS horas_peligro
# MAGIC FROM adb_ecoazul.gold.risk_predictions
# MAGIC WHERE prediction_ts = (SELECT MAX(prediction_ts) FROM adb_ecoazul.gold.risk_predictions)
# MAGIC GROUP BY zona_id
# MAGIC ORDER BY avg_risk_pred DESC;
