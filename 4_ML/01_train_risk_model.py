# Databricks notebook source
# MAGIC %md
# MAGIC ## Training Pipeline: Prediccion de Risk Score
# MAGIC
# MAGIC Entrena modelo ML para predecir risk_score usando **scikit-learn**.
# MAGIC PySpark MLlib tiene un bug en este Databricks Runtime que resetea maxBins,
# MAGIC por lo que usamos scikit-learn (Python nativo, sin JVM).
# MAGIC
# MAGIC Modelos disponibles:
# MAGIC - Gradient Boosted Trees (GBT) — recomendado
# MAGIC - Random Forest (RF)
# MAGIC - Linear Regression (baseline)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports y Configuracion

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import mlflow
import mlflow.sklearn
from datetime import datetime

CATALOG = "adb_ecoazul"
GOLD = f"{CATALOG}.gold"

# Configuracion de training
TRAIN_END_DATE = None  # None = hasta hoy, o especificar "YYYY-MM-DD"
TEST_SPLIT = 0.2
RANDOM_SEED = 42

# Configuracion de modelo
MODEL_TYPE = "GBT"  # GBT | RF | LR
MAX_DEPTH = 10
NUM_TREES = 100

# MLflow experiment
current_user = spark.sql("SELECT current_user()").collect()[0][0]
EXPERIMENT_NAME = f"/Users/{current_user}/risk_score_prediction"

# Desactivar autologging de Databricks para evitar interferencias
try:
    mlflow.autolog(disable=True)
    spark.conf.set("spark.databricks.mlflow.trackMLlib.enabled", "false")
except Exception:
    pass

print(f"Training Configuration:")
print(f"  MODEL_TYPE: {MODEL_TYPE}")
print(f"  MAX_DEPTH: {MAX_DEPTH}")
print(f"  NUM_TREES: {NUM_TREES}")
print(f"  TEST_SPLIT: {TEST_SPLIT}")
print(f"  RANDOM_SEED: {RANDOM_SEED}")
print(f"  EXPERIMENT: {EXPERIMENT_NAME}")
print(f"  Framework: scikit-learn (bypass PySpark MLlib bug)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Cargar Features

# COMMAND ----------

# Cargar tabla de features desde Spark
df_features = spark.table(f"{GOLD}.features_ml")

if TRAIN_END_DATE:
    df_features = df_features.where(
        F.to_date("timestamp_forecast") <= F.lit(TRAIN_END_DATE)
    )

print(f"Registros cargados: {df_features.count():,}")
print(f"Zonas: {df_features.select('zona_id').distinct().count()}")
print(f"Rango temporal: {df_features.agg(F.min('timestamp_forecast'), F.max('timestamp_forecast')).collect()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Definir Features

# COMMAND ----------

numeric_features = [
    # Base
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

    # Agregaciones 24h
    "wave_height_mean_24h",
    "wave_height_max_24h",
    "wave_height_std_24h",
    "wind_speed_mean_24h",
    "wind_speed_max_24h",
    "precipitation_sum_24h",

    # Agregaciones 6h
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

    # Temporales
    "hour_of_day",
    "day_of_week"
    # wave_wind_interaction y wave_current_interaction excluidos del vector de training:
    # correlación ~0.96 con risk_score → near data-leakage, el modelo ignoraba el resto.
    # Las columnas siguen en features_ml para análisis, pero no en el feature vector.
]

target = "risk_score"
all_features = numeric_features + ["zona_id"]

print(f"Numeric features: {len(numeric_features)}")
print(f"Categorical: zona_id")
print(f"Target: {target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Convertir a Pandas

# COMMAND ----------

# Seleccionar columnas necesarias y convertir a Pandas
columns_needed = all_features + [target, "timestamp_forecast"]
pdf = df_features.select(columns_needed).toPandas()

print(f"DataFrame Pandas: {pdf.shape[0]:,} filas x {pdf.shape[1]} columnas")
print(f"Memoria: {pdf.memory_usage(deep=True).sum() / 1e6:.1f} MB")

# Filtrar filas sin target
pdf = pdf[pdf[target].notna()]
pdf = pdf[(pdf[target] >= 0) & (pdf[target] <= 200)]
print(f"Despues de filtrar outliers: {pdf.shape[0]:,} filas")

if pdf.shape[0] < 100:
    raise Exception(
        f"Pocos datos: {pdf.shape[0]} filas. Necesitas al menos 100."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Encoding de zona_id

# COMMAND ----------

# LabelEncoder para zona_id (equivale a StringIndexer)
le_zona = LabelEncoder()
pdf["zona_id_encoded"] = le_zona.fit_transform(pdf["zona_id"].astype(str))

# Mostrar mapeo
zona_mapping = dict(zip(le_zona.classes_, le_zona.transform(le_zona.classes_)))
print("Mapeo zona_id:")
for zona, idx in sorted(zona_mapping.items(), key=lambda x: x[1]):
    print(f"  {zona} → {idx}")

# Features finales para el modelo
feature_cols = numeric_features + ["zona_id_encoded"]

print(f"\nTotal features para modelo: {len(feature_cols)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Split Train/Test

# COMMAND ----------

# Split temporal
pdf = pdf.sort_values("timestamp_forecast")
split_idx = int(len(pdf) * (1 - TEST_SPLIT))

train_pdf = pdf.iloc[:split_idx].copy()
test_pdf = pdf.iloc[split_idx:].copy()

# Preparar X, y
X_train = train_pdf[feature_cols].fillna(0)
y_train = train_pdf[target]
X_test = test_pdf[feature_cols].fillna(0)
y_test = test_pdf[target]

split_date = pdf.iloc[split_idx]["timestamp_forecast"]
print(f"Split TEMPORAL (fecha corte: {split_date})")
print(f"Train: {len(X_train):,} registros")
print(f"Test:  {len(X_test):,} registros")
print(f"Features: {X_train.shape[1]}")

if len(X_train) == 0:
    raise Exception("Dataset de training vacio.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Entrenar Modelo

# COMMAND ----------

# Crear modelo segun MODEL_TYPE
if MODEL_TYPE == "GBT":
    model = GradientBoostingRegressor(
        n_estimators=NUM_TREES,
        max_depth=MAX_DEPTH,
        random_state=RANDOM_SEED,
        learning_rate=0.1,
        subsample=0.8
    )
    model_desc = f"GradientBoosting (n_estimators={NUM_TREES}, max_depth={MAX_DEPTH})"

elif MODEL_TYPE == "RF":
    model = RandomForestRegressor(
        n_estimators=NUM_TREES,
        max_depth=MAX_DEPTH,
        random_state=RANDOM_SEED,
        n_jobs=-1
    )
    model_desc = f"RandomForest (n_estimators={NUM_TREES}, max_depth={MAX_DEPTH})"

elif MODEL_TYPE == "LR":
    model = LinearRegression()
    model_desc = "LinearRegression (baseline)"

else:
    raise ValueError(f"MODEL_TYPE invalido: {MODEL_TYPE}")

print(f"Modelo: {model_desc}")
print("Entrenando...")

model.fit(X_train, y_train)

print("Modelo entrenado OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Evaluar Metricas

# COMMAND ----------

# Predicciones
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

# Metricas en test
rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
mae = mean_absolute_error(y_test, y_pred_test)
r2 = r2_score(y_test, y_pred_test)

# Metricas en train (para detectar overfitting)
rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
mae_train = mean_absolute_error(y_train, y_pred_train)
r2_train = r2_score(y_train, y_pred_train)

print(f"{'='*50}")
print(f"METRICAS EN TEST:")
print(f"  RMSE: {rmse:.4f}")
print(f"  MAE:  {mae:.4f}")
print(f"  R2:   {r2:.4f}")
print(f"{'='*50}")
print(f"METRICAS EN TRAIN:")
print(f"  RMSE: {rmse_train:.4f}")
print(f"  MAE:  {mae_train:.4f}")
print(f"  R2:   {r2_train:.4f}")
print(f"{'='*50}")

if r2_train - r2 > 0.15:
    print("AVISO: Posible overfitting (R2 train >> R2 test). Considerar reducir MAX_DEPTH.")
elif rmse > 20:
    print("AVISO: RMSE alto. Considerar aumentar NUM_TREES o MAX_DEPTH.")
else:
    print("Metricas dentro de rango esperado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Guardar en MLflow

# COMMAND ----------

mlflow.set_experiment(EXPERIMENT_NAME)

with mlflow.start_run(run_name=f"{MODEL_TYPE}_{datetime.now().strftime('%Y%m%d_%H%M')}"):

    # Parametros
    mlflow.log_param("model_type", MODEL_TYPE)
    mlflow.log_param("max_depth", MAX_DEPTH)
    mlflow.log_param("num_trees", NUM_TREES)
    mlflow.log_param("test_split", TEST_SPLIT)
    mlflow.log_param("train_records", len(X_train))
    mlflow.log_param("test_records", len(X_test))
    mlflow.log_param("num_features", len(feature_cols))
    mlflow.log_param("framework", "scikit-learn")

    # Metricas
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("mae", mae)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("rmse_train", rmse_train)
    mlflow.log_metric("mae_train", mae_train)
    mlflow.log_metric("r2_train", r2_train)

    # Guardar modelo
    mlflow.sklearn.log_model(model, "model")

    run_id = mlflow.active_run().info.run_id
    print(f"Modelo y metricas guardados en MLflow")
    print(f"Run ID: {run_id}")
    print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Feature Importance

# COMMAND ----------

if MODEL_TYPE in ["GBT", "RF"]:
    importances = model.feature_importances_

    importance_df = pd.DataFrame({
        "feature": feature_cols,
        "importance": importances
    }).sort_values("importance", ascending=False)

    print("\nTOP 15 FEATURES MAS IMPORTANTES:")
    print("=" * 55)
    for _, row in importance_df.head(15).iterrows():
        bar = "█" * int(row["importance"] * 100)
        print(f"  {row['feature']:<30} {row['importance']:.4f} {bar}")
    print("=" * 55)

    # Crear Spark DataFrame para visualizacion SQL
    df_importance = spark.createDataFrame(importance_df)
    df_importance.createOrReplaceTempView("feature_importance")

    # Persistir a Gold para Power BI
    importance_df["model_type"] = MODEL_TYPE
    importance_df["run_id"]     = run_id
    importance_df["rank"]       = pd.array(range(1, len(importance_df) + 1), dtype="int32")  # INT32 para coincidir con Delta INT
    importance_df["created_at"] = pd.Timestamp.now()

    spark.createDataFrame(
        importance_df[["feature", "importance", "rank", "model_type", "run_id", "created_at"]]
    ).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD}.feature_importance")
    # overwriteSchema=true evita DELTA_FAILED_TO_MERGE_FIELDS si el schema cambió entre runs

    print(f"Feature importance guardada en {GOLD}.feature_importance ({len(importance_df)} features)")

else:
    # Para LR, usar coeficientes
    coef_df = pd.DataFrame({
        "feature": feature_cols,
        "coefficient": model.coef_
    }).sort_values("coefficient", ascending=False, key=abs)

    print("\nTOP 15 COEFICIENTES (valor absoluto):")
    for _, row in coef_df.head(15).iterrows():
        print(f"  {row['feature']:<30} {row['coefficient']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Analisis de Predicciones

# COMMAND ----------

# Crear DataFrame de analisis
test_pdf["risk_score_pred"] = y_pred_test
test_pdf["error"] = test_pdf["risk_score_pred"] - test_pdf[target]
test_pdf["abs_error"] = test_pdf["error"].abs()

# Estadisticas
print("ESTADISTICAS PREDICCIONES vs REALES:")
stats = pd.DataFrame({
    "real": y_test.describe(),
    "predicted": pd.Series(y_pred_test).describe()
}).round(2)
print(stats)

# Crear vista temporal en Spark para queries SQL
df_pred_analysis = spark.createDataFrame(
    test_pdf[["zona_id", "timestamp_forecast", target, "risk_score_pred", "error", "abs_error"]]
    .rename(columns={target: "risk_score_real"})
)
df_pred_analysis.createOrReplaceTempView("predictions_analysis")
print("\nVista temporal creada: predictions_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Guardar Modelo en Registry

# COMMAND ----------

model_name = "risk_score_predictor"

try:
    model_uri = f"runs:/{run_id}/model"

    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name=model_name
    )

    print(f"Modelo registrado: {model_name}")
    print(f"Version: {registered_model.version}")

except Exception as e:
    print(f"Registro opcional no disponible: {e}")
    print("(El modelo igual esta guardado en MLflow Experiments)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Resumen

# COMMAND ----------

training_summary = f"""
{'='*60}
TRAINING COMPLETADO
{'='*60}

Framework: scikit-learn (bypass PySpark MLlib maxBins bug)
Modelo: {MODEL_TYPE}

Parametros:
  max_depth:  {MAX_DEPTH}
  num_trees:  {NUM_TREES}
  seed:       {RANDOM_SEED}

Dataset:
  Train: {len(X_train):,} registros
  Test:  {len(X_test):,} registros
  Features: {len(feature_cols)} ({len(numeric_features)} numericas + zona_id)

Metricas (Test):
  RMSE: {rmse:.4f}
  MAE:  {mae:.4f}
  R2:   {r2:.4f}

Metricas (Train):
  RMSE: {rmse_train:.4f}
  R2:   {r2_train:.4f}

MLflow: {EXPERIMENT_NAME}

INTERPRETACION:
  RMSE < 10  → Excelente
  RMSE 10-20 → Buena
  RMSE > 20  → Ajustar hiperparametros

{'='*60}
"""

print(training_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Validaciones Finales

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Mejora vs baseline (predecir siempre la media)
# MAGIC SELECT
# MAGIC   ROUND(AVG(abs_error), 2) AS mae_modelo,
# MAGIC   ROUND(AVG(ABS(risk_score_real - (SELECT AVG(risk_score_real) FROM predictions_analysis))), 2) AS mae_baseline
# MAGIC FROM predictions_analysis;
