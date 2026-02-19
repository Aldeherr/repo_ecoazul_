# Databricks notebook source
# MAGIC %md
# MAGIC # VALIDADOR DE CALIDAD

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configuracion Inicial

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.sql import functions as F
import json, hashlib
from datetime import datetime

SCOPE = "keyvault-scope"
STORAGE_ACCOUNT = "dataecoazul"
CONTAINER = "bronze"

# Reglas de negocio marea (confirmadas)
EXPECTED_PUNTOS = 30
EXPECTED_POINTS_PER_PUNTO = 48
MIN_RANGE_H = 47.0

# ----------------------------
# 1) AUTH + PATH HELPERS
# ----------------------------
storage_key = dbutils.secrets.get(scope=SCOPE, key="storage-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", storage_key)

def get_path(container, path=""):
    """Construye la URL wasbs:// completa para acceder a Azure Blob Storage."""
    base = f"wasbs://{container}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    return f"{base}/{path.lstrip('/')}" if path else base

def lsR_parquet(path):
    """Recorre recursivamente un path en DBFS y retorna todos los archivos .parquet encontrados."""
    out = []
    for fi in dbutils.fs.ls(path):
        if fi.isFile():
            if fi.path.lower().endswith(".parquet"):
                out.append(fi.path)
        else:
            out.extend(lsR_parquet(fi.path))
    return out

def schema_fingerprint(schema) -> str:
    """Calcula el hash SHA256 del esquema Spark serializado como JSON ordenado."""
    j = json.dumps(schema.jsonValue(), sort_keys=True)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()

def read_expected_fp(source: str) -> str:
    """Lee el fingerprint de esquema esperado desde el Volume de contratos para la fuente dada."""
    p = CONTRACT_FP[source]
    return dbutils.fs.head(p).strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MAREA 

# COMMAND ----------

# MAGIC %md
# MAGIC ### FUNCIONES

# COMMAND ----------

def compute_marea_metrics(df):
    """
    Calcula métricas de calidad del parquet de Marea.

    Extrae los timestamps del array raw_data.heights por punto_id y calcula:
    - puntos_cnt: número de puntos de muestreo distintos.
    - rng: DataFrame con min/max timestamp, conteo y rango horario por punto_id.
    - dup_heights: DataFrame con timestamps duplicados por punto_id.

    Returns:
        dict con claves: puntos_cnt (int), rng (DataFrame), dup_heights (DataFrame).
    """
    heights = (df
        .select("punto_id", F.explode_outer("raw_data.heights").alias("h"))
        .select(
            "punto_id",
            F.col("h.timestamp").alias("ts"),
            F.col("h.height").alias("height")
        )
        .filter(F.col("ts").isNotNull())
    )

    rng = (heights.groupBy("punto_id")
        .agg(
            F.min("ts").alias("min_ts"),
            F.max("ts").alias("max_ts"),
            F.count("*").alias("n_points")
        )
        .withColumn("range_hours", (F.col("max_ts") - F.col("min_ts")) / 3600.0)
    )

    dup_heights = (heights
        .groupBy("punto_id", "ts").count()
        .filter(F.col("count") > 1)
    )

    puntos_cnt = df.select("punto_id").distinct().count()

    return {"puntos_cnt": puntos_cnt, "rng": rng, "dup_heights": dup_heights}

def validate_marea_content(metrics,
                           expected_puntos=30,
                           expected_points=48,
                           min_range_h=47.0):
    """
    Valida las métricas calculadas de Marea contra umbrales de negocio.

    Reglas evaluadas:
    - PUNTOS_COUNT: número de puntos distintos debe ser exactamente expected_puntos.
    - RANGE_OR_POINTS: cada punto debe tener >= expected_points registros y >= min_range_h horas.
    - DUPLICATES_HEIGHTS: no deben existir timestamps duplicados por punto_id.

    Returns:
        list de dicts con las violaciones encontradas (vacía si todo pasa).
    """
    violations = []

    if metrics["puntos_cnt"] != expected_puntos:
        violations.append({
            "rule": "PUNTOS_COUNT",
            "found": metrics["puntos_cnt"],
            "expected": expected_puntos
        })

    rng = metrics["rng"]
    bad_cnt = rng.filter(
        (F.col("n_points") < expected_points) |
        (F.col("range_hours") < min_range_h)).count()

    if bad_cnt > 0:
        violations.append({
            "rule": "RANGE_OR_POINTS",
            "bad_puntos": bad_cnt,
            "expected_points": expected_points,
            "min_range_h": min_range_h
        })

    dup_cnt = metrics["dup_heights"].count()
    if dup_cnt > 0:
        violations.append({
            "rule": "DUPLICATES_HEIGHTS",
            "dup_keys": dup_cnt
        })

    return violations

def validate_marea_path(path,
                        expected_schema_fp,
                        expected_puntos=30,
                        expected_points=48,
                        min_range_h=47.0,
                        debug_display=True):
    """
    Valida completamente un parquet de Marea: schema contract + reglas de contenido.

    Lanza Exception si el schema no coincide con el contrato o si hay violaciones de contenido.
    Con debug_display=True muestra los DataFrames de rangos y duplicados antes de lanzar.

    Returns:
        dict con status='PASS', path, schema_fp y puntos_cnt si supera todas las validaciones.
    """
    df = spark.read.parquet(path)

    fp = schema_fingerprint(df.schema)
    if fp != expected_schema_fp:
        raise Exception(f"MAREA FAIL: SCHEMA_MISMATCH path={path} fp={fp} expected={expected_schema_fp}")

    metrics = compute_marea_metrics(df)
    violations = validate_marea_content(metrics, expected_puntos, expected_points, min_range_h)

    if violations:
        if debug_display:
            display(metrics["rng"].orderBy("punto_id"))
            display(metrics["dup_heights"].orderBy("punto_id", "ts"))
        raise Exception(f"MAREA FAIL: {violations}")

    return {"status": "PASS", "path": path, "schema_fp": fp, "puntos_cnt": metrics["puntos_cnt"]}


# COMMAND ----------

# MAGIC %md
# MAGIC ## STORMGLASS

# COMMAND ----------

# MAGIC %md
# MAGIC ### FUNCIONES

# COMMAND ----------

def compute_stormglass_metrics(df):
    """
    Calcula métricas de calidad del parquet de StormGlass.

    Extrae el array raw_data.hours por punto_id y calcula:
    - puntos_cnt: número de puntos de muestreo distintos.
    - rng: DataFrame con min/max timestamp, conteo y rango horario por punto_id.
    - dup_hours: DataFrame con timestamps duplicados por punto_id.

    Returns:
        dict con claves: puntos_cnt (int), rng (DataFrame), dup_hours (DataFrame).
    """
    hours = (
        df.select("punto_id", F.explode_outer("raw_data.hours").alias("h"))
          .select(
              "punto_id",
              F.col("h.time").alias("time_raw"),
              F.to_timestamp(F.col("h.time")).alias("ts")
          )
          .filter(F.col("ts").isNotNull())
    )

    puntos_cnt = df.select("punto_id").distinct().count()

    rng = (
        hours.groupBy("punto_id")
             .agg(
                 F.min("ts").alias("min_ts"),
                 F.max("ts").alias("max_ts"),
                 F.count("*").alias("n_points")
             )
             .withColumn(
                 "range_hours",
                 (F.col("max_ts").cast("long") - F.col("min_ts").cast("long")) / 3600.0
             )
    )

    dup_hours = (
        hours.groupBy("punto_id", "ts").count()
             .filter(F.col("count") > 1)
    )

    return {"puntos_cnt": puntos_cnt, "rng": rng, "dup_hours": dup_hours}

def validate_stormglass_content(metrics,
                                expected_puntos=EXPECTED_PUNTOS,
                                expected_points=EXPECTED_POINTS_PER_PUNTO,
                                min_range_h=MIN_RANGE_H):
    """
    Valida las métricas calculadas de StormGlass contra umbrales de negocio.

    Reglas evaluadas:
    - PUNTOS_COUNT: número de puntos distintos debe coincidir con expected_puntos.
    - RANGE_OR_POINTS: cada punto debe tener >= expected_points horas y >= min_range_h de ventana.
    - DUPLICATES_HOURS: no deben existir timestamps duplicados por punto_id.

    Returns:
        list de dicts con las violaciones encontradas (vacía si todo pasa).
    """
    violations = []

    if metrics["puntos_cnt"] != expected_puntos:
        violations.append({
            "rule": "PUNTOS_COUNT",
            "found": metrics["puntos_cnt"],
            "expected": expected_puntos
        })

    rng = metrics["rng"]
    bad_cnt = rng.filter(
        (F.col("n_points") < expected_points) |
        (F.col("range_hours") < min_range_h)
    ).count()

    if bad_cnt > 0:
        violations.append({
            "rule": "RANGE_OR_POINTS",
            "bad_puntos": bad_cnt,
            "expected_points": expected_points,
            "min_range_h": min_range_h
        })

    dup_cnt = metrics["dup_hours"].count()
    if dup_cnt > 0:
        violations.append({
            "rule": "DUPLICATES_HOURS",
            "dup_keys": dup_cnt
        })

    return violations

def validate_stormglass_path(path,
                             expected_schema_fp,
                             expected_puntos=EXPECTED_PUNTOS,
                             expected_points=EXPECTED_POINTS_PER_PUNTO,
                             min_range_h=MIN_RANGE_H,
                             debug_display=True):
    """
    Valida completamente un parquet de StormGlass: schema contract + reglas de contenido.

    Lanza Exception si el schema no coincide con el contrato o si hay violaciones de contenido.
    Con debug_display=True muestra los DataFrames de rangos y duplicados antes de lanzar.

    Returns:
        dict con status='PASS', path, schema_fp y puntos_cnt si supera todas las validaciones.
    """
    df = spark.read.parquet(path)

    fp = schema_fingerprint(df.schema)
    if fp != expected_schema_fp:
        raise Exception(f"STORMGLASS FAIL: SCHEMA_MISMATCH path={path} fp={fp} expected={expected_schema_fp}")

    metrics = compute_stormglass_metrics(df)
    violations = validate_stormglass_content(metrics, expected_puntos, expected_points, min_range_h)

    if violations:
        if debug_display:
            display(metrics["rng"].orderBy("punto_id"))
            display(metrics["dup_hours"].orderBy("punto_id", "ts"))
        raise Exception(f"STORMGLASS FAIL: {violations}")

    return {"status": "PASS", "path": path, "schema_fp": fp, "puntos_cnt": metrics["puntos_cnt"]}


# COMMAND ----------

# MAGIC %md
# MAGIC ## COPERNICUS
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### FUNCIONES

# COMMAND ----------


def compute_copernicus_metrics(df):
    """
    Calcula métricas de calidad del parquet de Copernicus.

    Evalúa:
    - total: número total de registros.
    - zonas_cnt: número de zonas distintas presentes.
    - missing_zonas: zonas esperadas ausentes en el parquet.
    - dup_cnt: duplicados por clave natural (zona_id, time, lat, lon, tipo_dato).
    - nulls: conteo de nulos por columna crítica definida en REQUIRED_COLS_COP.

    Returns:
        dict con claves: total, zonas_cnt, missing_zonas, dup_cnt, nulls, dup_df (DataFrame).
    """
    total = df.count()

    zonas_cnt = df.select("zona_id").where(F.col("zona_id").isNotNull()).distinct().count()

    missing_zonas = (
        spark.createDataFrame([(z,) for z in EXPECTED_ZONAS_COP], "zona_id string")
             .join(df.select("zona_id").distinct(), on="zona_id", how="left_anti")
             .count()
    )

    # Duplicados por clave natural
    dup = (df.groupBy("zona_id","time","latitude","longitude","tipo_dato").count()
             .filter(F.col("count") > 1))
    dup_cnt = dup.count()

    # Nulos en columnas críticas
    nulls = {}
    for c in REQUIRED_COLS_COP:
        nulls[c] = df.filter(F.col(c).isNull()).count()

    return {"total": total, "zonas_cnt": zonas_cnt, "missing_zonas": missing_zonas, "dup_cnt": dup_cnt, "nulls": nulls, "dup_df": dup}

def validate_copernicus_path(path, expected_schema_fp, expected_zonas, required_cols, min_zone, debug_display=False):
    """
    Valida completamente un parquet de Copernicus con 6 reglas en cascada.

    Reglas (en orden):
    1. Schema contract: fingerprint SHA256 debe coincidir con el contrato almacenado.
    2. Columnas requeridas: todas deben estar presentes en el DataFrame.
    3. Archivo no vacío.
    4. Sin nulos en columnas requeridas.
    5. Zonas presentes: al menos min_zone zonas de expected_zonas (faltantes = SOFT warning).
    6. Sin duplicados por clave natural (zona_id, time, latitude, longitude, tipo_dato).

    Returns:
        dict con status='PASS', path, schema_fp, puntos_cnt, missing_cnt y warnings.
    """
    df = spark.read.parquet(path)

    # 1) Schema contract
    fp = schema_fingerprint(df.schema)
    if fp != expected_schema_fp:
        raise Exception(f"COPERNICUS FAIL SCHEMA_MISMATCH path={path} fp={fp} expected={expected_schema_fp}")

    # 2) Required columns present
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise Exception(f"COPERNICUS FAIL MISSING_COLUMNS {missing_cols}")

    # 3) Not empty
    total = df.count()
    if total <= 0:
        raise Exception("COPERNICUS FAIL EMPTY_FILE")

    # 4) Required cols not null (hard)
    null_viol = {}
    for c in required_cols:
        n = df.filter(F.col(c).isNull()).count()
        if n > 0:
            null_viol[c] = n
    if null_viol:
        raise Exception(f"COPERNICUS FAIL NULLS_IN_REQUIRED {null_viol}")

    # 5) Expected zonas exist (soft/hard)
    expected_set = set(expected_zonas)  # evita duplicados en la lista
    present_set = set(
        r["zona_id"] for r in df.select("zona_id").distinct().collect()
        if r["zona_id"] is not None
    )

    missing_list = sorted(list(expected_set - present_set))
    missing_cnt = len(missing_list)

    if len(present_set) < min_zone:
        raise Exception(f"COPERNICUS FAIL ZONAS_PRESENTES present={len(present_set)} missing={missing_list}")

    warnings = None
    if missing_cnt > 0:
        warnings = f"SOFT:MISSING_ZONAS missing={missing_list}"

    # 6) Duplicates by natural key (hard)
    dup_df = (
        df.groupBy("zona_id", "time", "latitude", "longitude", "tipo_dato")
          .count()
          .filter(F.col("count") > 1)
    )
    dup_cnt = dup_df.count()
    if dup_cnt > 0:
        if debug_display:
            display(dup_df)
        raise Exception(f"COPERNICUS FAIL DUPLICATES dup_keys={dup_cnt}")

    # Return: puntos_cnt = number of zonas found (for logging)
    zonas_cnt = df.select("zona_id").distinct().count()
    return {"status": "PASS", 
            "path": path, 
            "schema_fp": fp, 
            "puntos_cnt": zonas_cnt,  
            "missing_cnt": missing_cnt,
            "warnings": warnings
            }

# COMMAND ----------

EXPECTED_ZONAS_COP = ["BT01","CO01","GG01","PVX01","BH01","GM01","PD01","GP01","PCH01","GCH01"]  # según tu notebook
REQUIRED_COLS_COP = ["time","latitude","longitude","zona_id","nombre_zona","tipo_dato"]  # en tu normalización
MIN_ZONAS = 8

# COMMAND ----------

# MAGIC %md
# MAGIC ## VALIDACION FINAL

# COMMAND ----------

dbutils.widgets.text("source", "")
dbutils.widgets.text("path", "")

source = dbutils.widgets.get("source").strip().lower()
path_param = dbutils.widgets.get("path").strip()

CONTRACT_FP = {
    "marea": "/Volumes/adb_ecoazul/config/contracts/marea/schema_fp_v1.txt",
    "stormglass": "/Volumes/adb_ecoazul/config/contracts/stormglass/schema_fp_v1.txt",
    "copernicus": "/Volumes/adb_ecoazul/config/contracts/copernicus/schema_fp_v1.txt"
}

expected_schema_fp = dbutils.fs.head(CONTRACT_FP[source]).strip()

# COMMAND ----------

if not source:
    raise Exception("Missing widget 'source' (marea/stormglass/copernicus)")
if source not in CONTRACT_FP:
    raise Exception(f"Unsupported source={source}. Expected one of: {list(CONTRACT_FP.keys())}")

if not path_param:
    raise Exception("Missing widget 'path' (always pass it from bronze-landed)")

expected_fp = read_expected_fp(source)

# COMMAND ----------

TABLE = "adb_ecoazul.dq.validation_results"

def log_result(source, status, path, schema_fp, puntos_cnt=None, bad_puntos=0, dup_keys=0, violations=None):
    """
    Registra el resultado de una validación en la tabla Delta adb_ecoazul.dq.validation_results.

    Args:
        source: fuente de datos (marea / stormglass / copernicus).
        status: resultado de la validación ('PASS' o 'FAIL').
        path: path del parquet validado en Azure Blob Storage.
        schema_fp: fingerprint SHA256 del schema encontrado en el parquet.
        puntos_cnt: número de puntos o zonas validados.
        bad_puntos: número de puntos con problemas de rango o conteo.
        dup_keys: número de claves duplicadas detectadas.
        violations: string con descripción de violaciones o warnings (None si PASS).
    """
    row = [(datetime.utcnow(), source, path, status, schema_fp, puntos_cnt, bad_puntos, dup_keys, violations)]
    df_log = spark.createDataFrame(row, "run_at timestamp, source string, path string, status string, schema_fp string, puntos_cnt int, bad_puntos int, dup_keys int, violations string")
    df_log.write.mode("append").saveAsTable(TABLE)

# COMMAND ----------

# HAPPY PATH
try:
    if source == "marea":
        result = validate_marea_path(
            path=path_param,
            expected_schema_fp=expected_schema_fp,
            expected_puntos=EXPECTED_PUNTOS,
            expected_points=EXPECTED_POINTS_PER_PUNTO,
            min_range_h=MIN_RANGE_H,
            debug_display=False
        )

    elif source == "stormglass":
        result = validate_stormglass_path(
            path=path_param,
            expected_schema_fp=expected_schema_fp,
            expected_puntos=EXPECTED_PUNTOS,      # 30
            expected_points=EXPECTED_POINTS_PER_PUNTO,        # 48
            min_range_h=MIN_RANGE_H,
            debug_display=False
        )
    elif source == "copernicus":
        result = validate_copernicus_path(
            path=path_param,
            expected_schema_fp=expected_schema_fp,
            expected_zonas=EXPECTED_ZONAS_COP,
            required_cols=REQUIRED_COLS_COP,
            min_zone=MIN_ZONAS,
            debug_display=False
        )

    else:
        raise Exception(f"Unsupported source={source}")

    log_result(source, 
               "PASS", 
               path_param, 
               result["schema_fp"], 
               puntos_cnt=result["puntos_cnt"], 
               bad_puntos=result.get("missing_cnt", 0),
               violations = result.get("warnings") 
               )

except Exception as e:
    # Guardando el error
    log_result(source, "FAIL", path_param, expected_schema_fp, violations=str(e))
    raise

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_ecoazul.dq.validation_results
# MAGIC -- WHERE status == 'FAIL'
# MAGIC ORDER BY run_at DESC
# MAGIC LIMIT 50;
# MAGIC

