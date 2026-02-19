# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Imports y Parametros
# MAGIC
# MAGIC AFECTADO por el fix (seccion 8): RUN_LOOKBACK_DAYS vale 2 aqui. El fix lo eleva
# MAGIC a 3 para que Silver incluya el run del dia anterior de Copernicus, que es el unico
# MAGIC que tiene datos del dia en curso dado el lag minimo de 1 dia del servicio (H-1b).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "adb_ecoazul"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

# Ventana para construir el Gold (en horas hacia adelante desde ahora)
HOURS_FORWARD = 72

# Lookback de runs (en días) para capturar la última corrida válida
RUN_LOOKBACK_DAYS = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear schema y tabla Gold
# MAGIC
# MAGIC No afectado por el fix. El DDL de la tabla y sus columnas no cambian.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD}.forecast_zona_hora (
  zona_id STRING NOT NULL,
  timestamp_forecast TIMESTAMP NOT NULL,

  -- StormGlass (agregado por zona/hora)
  wave_height_m DOUBLE,
  wind_speed_ms DOUBLE,
  air_temperature_c DOUBLE,
  precipitation_mm DOUBLE,
  precipitation_3h_mm DOUBLE,
  cloud_cover_pct DOUBLE,
  visibility_km DOUBLE,
  stormglass_n_puntos INT,
  stormglass_run_ts TIMESTAMP,

  -- Marea (agregado por zona/hora)
  altura_marea_m DOUBLE,
  tipo_proximo_cambio STRING,
  marea_n_puntos INT,
  marea_run_ts TIMESTAMP,

  -- Copernicus (agregado por zona/hora)
  temperatura_mar_c DOUBLE,
  salinidad_psu DOUBLE,
  corriente_u_ms DOUBLE,
  corriente_v_ms DOUBLE,
  copernicus_n_grillas INT,
  copernicus_run_ts TIMESTAMP,

  updated_at TIMESTAMP NOT NULL,

  CONSTRAINT pk_gold PRIMARY KEY (zona_id, timestamp_forecast)
) USING DELTA
""")

print("Tabla creada/verificada:", f"{GOLD}.forecast_zona_hora")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Agregar columna precipitation_3h_mm si no existe
# MAGIC ALTER TABLE adb_ecoazul.gold.forecast_zona_hora
# MAGIC ADD COLUMN IF NOT EXISTS precipitation_3h_mm DOUBLE
# MAGIC COMMENT 'Precipitacion acumulada proximas 3 horas (mm)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cargar Silver
# MAGIC
# MAGIC AFECTADO por el fix (seccion 8): Copernicus se carga aqui con RUN_LOOKBACK_DAYS=2,
# MAGIC lo que puede excluir el run del dia anterior si esta justo en el limite. La seccion 8
# MAGIC recarga Copernicus con RUN_LOOKBACK_DAYS=3 para garantizar la inclusion del run de
# MAGIC ayer, que es el que contiene datos del dia en curso (H-1b).

# COMMAND ----------

def get_last_run_safe(df, source_name: str):
    """
    Obtiene el run_ts mas reciente disponible en un DataFrame de Silver.

    Si no existe ningun run dentro del lookback configurado, retorna None
    y el DataFrame completo sera usado sin filtrar por run.

    Args:
        df: DataFrame de Silver con columna run_ts.
        source_name: Nombre de la fuente para mensajes de log.

    Returns:
        Timestamp del run mas reciente, o None si el DataFrame esta vacio.
    """
    row = df.select(F.max("run_ts").alias("run")).collect()[0]
    last_run = row["run"]
    if last_run is None:
        print(f" {source_name}: no hay run_ts en el lookback. Se usara el DF completo")
    return last_run

# COMMAND ----------

now_ts = F.current_timestamp()
min_run_date = F.date_sub(F.current_date(), RUN_LOOKBACK_DAYS)

# StormGlass: tabla por punto/hora/run
df_sg = spark.table(f"{SILVER}.fact_stormglass_forecast") \
    .where(F.to_date("run_ts") >= min_run_date)

# Marea: serie 15min por punto/run
df_ma = spark.table(f"{SILVER}.fact_marea_forecast") \
    .where(F.to_date("run_ts") >= min_run_date)

# Copernicus: grilla por zona/hora/run
df_cp = spark.table(f"{SILVER}.fact_copernicus_forecast") \
    .where(F.to_date("run_ts") >= min_run_date)

# Tomar el último run por fuente
sg_last_run = get_last_run_safe(df_sg, "StormGlass")
ma_last_run = get_last_run_safe(df_ma, "Marea")
cp_last_run = get_last_run_safe(df_cp, "Copernicus")

print("StormGlass last run:", sg_last_run)
print("Marea last run:", ma_last_run)
print("Copernicus last run:", cp_last_run)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregaciones por zona/hora
# MAGIC
# MAGIC AFECTADO por el fix (seccion 8) en lo que respecta a Copernicus:
# MAGIC
# MAGIC - H-1b: cp_last_run usa MAX(run_ts) global. Si el run mas reciente es el de hoy,
# MAGIC   ese run no tiene datos del dia de hoy por el lag de Copernicus, y HOY quedara
# MAGIC   con salinidad y corrientes NULL en Gold.
# MAGIC
# MAGIC - H-1a: cp_zone agrupa por timestamp exacto. Como Copernicus solo tiene datos a
# MAGIC   las 00:00 UTC, unicamente esa hora recibe valores; las otras 23 horas del dia
# MAGIC   quedaran NULL al hacer el JOIN en la seccion siguiente.
# MAGIC
# MAGIC StormGlass y Marea no estan afectados y sus agregaciones se reutilizan en la seccion 8.

# COMMAND ----------

df_sg_last = df_sg if sg_last_run is None else df_sg.where(F.col("run_ts") == F.lit(sg_last_run))
df_ma_last = df_ma if ma_last_run is None else df_ma.where(F.col("run_ts") == F.lit(ma_last_run))
df_cp_last = df_cp if cp_last_run is None else df_cp.where(F.col("run_ts") == F.lit(cp_last_run))

# COMMAND ----------

df_ma_last.limit(3).show(truncate=False)

# COMMAND ----------


# StormGlass agregado por zona/hora para el último run
sg_zone = df_sg_last \
    .groupBy("zona_id", "timestamp_forecast") \
    .agg(
        F.avg("wave_height_m").alias("wave_height_m"),
        F.avg("wind_speed_ms").alias("wind_speed_ms"),
        F.avg("air_temperature_c").alias("air_temperature_c"),
        F.avg("precipitation_mm").alias("precipitation_mm"),
        F.avg("cloud_cover_pct").alias("cloud_cover_pct"),
        F.avg("visibility_km").alias("visibility_km"),
        F.countDistinct("punto_id").alias("stormglass_n_puntos"),
        F.max("run_ts").alias("stormglass_run_ts"),
    )
# Marea: truncar a hora y agregar por zona/hora para el último run
ma_zone = df_ma_last \
    .withColumn("timestamp_forecast", F.date_trunc("hour", F.col("timestamp_forecast"))) \
    .groupBy("zona_id", "timestamp_forecast") \
    .agg(
        F.avg("altura_marea_m").alias("altura_marea_m"),
        F.first("tipo_proximo_cambio", ignorenulls=True).alias("tipo_proximo_cambio"),
        F.count("*").alias("marea_n_puntos"),
        F.max("run_ts").alias("marea_run_ts"),
    )

# Copernicus: agregar por zona/hora para el último run
cp_zone = df_cp_last \
    .groupBy("zona_id", "timestamp_forecast") \
    .agg(
        F.avg("temperatura_mar_c").alias("temperatura_mar_c"),
        F.avg("salinidad_psu").alias("salinidad_psu"),
        F.avg("corriente_u_ms").alias("corriente_u_ms"),
        F.avg("corriente_v_ms").alias("corriente_v_ms"),
        F.count("*").alias("copernicus_n_grillas"),
        F.max("run_ts").alias("copernicus_run_ts"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Unir todo y escribir Gold (MERGE por PK)
# MAGIC
# MAGIC AFECTADO por el fix (seccion 8) en dos puntos:
# MAGIC
# MAGIC - H-1a (base union): cp_zone entra en la union base. Como Copernicus solo tiene
# MAGIC   una fila por zona a las 00:00, esto genera filas fantasma en la base que no
# MAGIC   corresponden a la resolucion horaria de StormGlass y Marea. La seccion 8
# MAGIC   construye la base solo con StormGlass y Marea.
# MAGIC
# MAGIC - H-1a (JOIN): el JOIN con Copernicus se hace por timestamp exacto, dejando
# MAGIC   23 de 24 horas sin datos oceanicos. La seccion 8 une por DATE para propagar
# MAGIC   el dato diario a todas las horas del dia.

# COMMAND ----------

# Universo de zona/hora = unión de las tres fuentes
base = sg_zone.select("zona_id", "timestamp_forecast") \
    .unionByName(ma_zone.select("zona_id", "timestamp_forecast")) \
    .unionByName(cp_zone.select("zona_id", "timestamp_forecast")) \
    .dropDuplicates(["zona_id", "timestamp_forecast"])

base = base.where(
    (F.col("timestamp_forecast") >= F.current_timestamp()) &
    (F.col("timestamp_forecast") < F.expr(f"current_timestamp() + INTERVAL {HOURS_FORWARD} HOURS"))
)

df_gold = base \
    .join(sg_zone, ["zona_id", "timestamp_forecast"], "left") \
    .join(ma_zone, ["zona_id", "timestamp_forecast"], "left") \
    .join(cp_zone, ["zona_id", "timestamp_forecast"], "left") \
    .withColumn("updated_at", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Calcular Precipitation Acumulado 3H
# MAGIC
# MAGIC No afectado por el fix. La logica de ventana rowsBetween(0, 2) es correcta.
# MAGIC Se recalcula en la seccion 8 sobre el DataFrame corregido de Copernicus.

# COMMAND ----------

# Definir ventana: proximas 3 horas por zona
w_3h = (
    Window
    .partitionBy("zona_id")
    .orderBy("timestamp_forecast")
    .rowsBetween(0, 2)  # Hora actual + 2 siguientes = 3 horas
)

# Agregar columna acumulada al DataFrame gold
df_gold = df_gold.withColumn(
    "precipitation_3h_mm",
    F.round(F.sum(F.coalesce(F.col("precipitation_mm"), F.lit(0.0))).over(w_3h), 1)
)

print("precipitation_3h_mm calculado con ventana 3h")

# COMMAND ----------

df_gold.createOrReplaceTempView("tmp_gold_forecast_zona_hora")

spark.sql(f"""
MERGE INTO {GOLD}.forecast_zona_hora AS t
USING tmp_gold_forecast_zona_hora AS s
ON t.zona_id = s.zona_id AND t.timestamp_forecast = s.timestamp_forecast
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("MERGE completado:", f"{GOLD}.forecast_zona_hora")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Post-check
# MAGIC
# MAGIC AFECTADO por el fix: las queries aqui verifican conteos generales y muestras de filas,
# MAGIC pero no validan la cobertura horaria de Copernicus. Despues de ejecutar el fix de la
# MAGIC seccion 8, usar las queries de la seccion 9 para confirmar que salinidad y corrientes
# MAGIC cubren todas las horas del dia y no solo las 00:00.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE adb_ecoazul.gold.forecast_zona_hora;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirmar que el MERGE
# MAGIC SELECT COUNT(*) AS total_filas
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Conteo por zona
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   COUNT(*) AS horas
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC GROUP BY zona_id
# MAGIC ORDER BY zona_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirmar ventana operativa (NOW → +72h)
# MAGIC SELECT
# MAGIC   MIN(timestamp_forecast) AS min_ts,
# MAGIC   MAX(timestamp_forecast) AS max_ts,
# MAGIC   current_timestamp()     AS now_ts
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS filas,
# MAGIC   COUNT(wave_height_m) AS con_oleaje
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS filas,
# MAGIC   COUNT(altura_marea_m) AS con_marea,
# MAGIC   COUNT(tipo_proximo_cambio) AS con_tipo
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS filas,
# MAGIC   COUNT(temperatura_mar_c) AS con_temp_mar,
# MAGIC   COUNT(corriente_u_ms) AS con_corriente
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MIN(updated_at) AS min_updated,
# MAGIC   MAX(updated_at) AS max_updated
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC WHERE zona_id = 'BH01'
# MAGIC ORDER BY timestamp_forecast
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC ORDER BY timestamp_forecast;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE adb_ecoazul.gold.forecast_zona_hora

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pruebas Precipitation Acumulado 3H
# MAGIC
# MAGIC No afectado por el fix. Estas pruebas validan la ventana de acumulacion 3h
# MAGIC que estaba correcta desde el inicio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### PRUEBAS ANTES/DESPUES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 1: Verificar que existe precipitation_3h_mm
# MAGIC DESCRIBE adb_ecoazul.gold.forecast_zona_hora;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 2: Ver valores calculados con validacion
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   timestamp_forecast,
# MAGIC   ROUND(precipitation_mm, 2) AS rate_mm_h,
# MAGIC   ROUND(precipitation_3h_mm, 2) AS acum_3h_mm,
# MAGIC   CASE
# MAGIC     WHEN precipitation_3h_mm >= precipitation_mm THEN 'OK'
# MAGIC     WHEN precipitation_3h_mm IS NULL AND precipitation_mm IS NULL THEN 'NULL'
# MAGIC     ELSE 'ERROR'
# MAGIC   END AS validacion
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC WHERE zona_id = 'BT01'
# MAGIC   AND timestamp_forecast >= current_timestamp()
# MAGIC ORDER BY timestamp_forecast
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 3: Verificar logica de ventana (calculo manual vs automatico)
# MAGIC WITH datos AS (
# MAGIC   SELECT
# MAGIC     zona_id,
# MAGIC     timestamp_forecast,
# MAGIC     precipitation_mm,
# MAGIC     precipitation_3h_mm,
# MAGIC     LAG(precipitation_mm, 0) OVER (PARTITION BY zona_id ORDER BY timestamp_forecast) +
# MAGIC     COALESCE(LEAD(precipitation_mm, 1) OVER (PARTITION BY zona_id ORDER BY timestamp_forecast), 0) +
# MAGIC     COALESCE(LEAD(precipitation_mm, 2) OVER (PARTITION BY zona_id ORDER BY timestamp_forecast), 0)
# MAGIC     AS acum_manual
# MAGIC   FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC   WHERE zona_id = 'BT01'
# MAGIC     AND timestamp_forecast >= current_timestamp()
# MAGIC )
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   timestamp_forecast,
# MAGIC   ROUND(precipitation_mm, 1) AS rate,
# MAGIC   ROUND(precipitation_3h_mm, 1) AS acum_calculado,
# MAGIC   ROUND(acum_manual, 1) AS acum_esperado,
# MAGIC   CASE
# MAGIC     WHEN ABS(precipitation_3h_mm - acum_manual) < 0.1 THEN 'MATCH'
# MAGIC     ELSE 'DIFF'
# MAGIC   END AS validacion
# MAGIC FROM datos
# MAGIC ORDER BY timestamp_forecast
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 4: Caso sin lluvia (0 mm)
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   timestamp_forecast,
# MAGIC   precipitation_mm,
# MAGIC   precipitation_3h_mm
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC WHERE precipitation_mm = 0
# MAGIC   AND timestamp_forecast >= current_timestamp()
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 5: Ultima hora del forecast (solo 1 hora disponible)
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   timestamp_forecast,
# MAGIC   ROUND(precipitation_mm, 1) AS rate,
# MAGIC   ROUND(precipitation_3h_mm, 1) AS acum_3h
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC WHERE zona_id = 'BT01'
# MAGIC ORDER BY timestamp_forecast DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 6: Estadisticas generales precipitation_3h_mm
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_filas,
# MAGIC   COUNT(precipitation_mm) AS con_precip_rate,
# MAGIC   COUNT(precipitation_3h_mm) AS con_precip_3h,
# MAGIC   ROUND(MIN(precipitation_3h_mm), 2) AS min_3h,
# MAGIC   ROUND(MAX(precipitation_3h_mm), 2) AS max_3h,
# MAGIC   ROUND(AVG(precipitation_3h_mm), 2) AS avg_3h
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PRUEBA 7: Comparacion por zona
# MAGIC SELECT
# MAGIC   zona_id,
# MAGIC   COUNT(*) AS horas,
# MAGIC   ROUND(AVG(precipitation_mm), 2) AS avg_rate,
# MAGIC   ROUND(AVG(precipitation_3h_mm), 2) AS avg_acum_3h,
# MAGIC   ROUND(MAX(precipitation_3h_mm), 2) AS max_acum_3h
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC WHERE timestamp_forecast >= current_timestamp()
# MAGIC GROUP BY zona_id
# MAGIC ORDER BY zona_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Fix H-1a y H-1b: JOIN por Fecha y Mejor Run por Zona-Dia
# MAGIC
# MAGIC AFECTADO. Este bloque corrige de forma permanente la logica de Copernicus
# MAGIC en las secciones 3, 4 y 5. El MERGE al final sobreescribe el resultado anterior.
# MAGIC
# MAGIC **H-1a**: el JOIN exacto por timestamp entre Gold y Copernicus produce NULL en
# MAGIC 23 de 24 horas porque Copernicus P1D-m entrega un solo dato por zona por dia
# MAGIC a las 00:00 UTC. La correccion une por DATE para propagar ese dato a todas las horas.
# MAGIC
# MAGIC **H-1b**: Copernicus tiene un lag minimo de 1 dia. La descarga del dia de hoy contiene
# MAGIC datos desde manana. Para cubrir el dia de hoy se necesita el run del dia anterior,
# MAGIC que si tiene datos del dia en curso. La correccion usa RUN_LOOKBACK_DAYS=3 y selecciona
# MAGIC el run mas reciente disponible para cada par (zona_id, fecha).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Recargar Copernicus con lookback ampliado
# MAGIC
# MAGIC AFECTADO (H-1b). Se aumenta RUN_LOOKBACK_DAYS a 3 para garantizar que el run
# MAGIC del dia anterior este dentro de la ventana de lectura de Silver.

# COMMAND ----------

RUN_LOOKBACK_DAYS = 3
min_run_date_fix = F.date_sub(F.current_date(), RUN_LOOKBACK_DAYS)

df_cp_fix = (
    spark.table(f"{SILVER}.fact_copernicus_forecast")
    .where(F.to_date("run_ts") >= min_run_date_fix)
)

print(f"Copernicus fix: {df_cp_fix.count()} registros en lookback de {RUN_LOOKBACK_DAYS} dias")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 Seleccionar el mejor run por (zona_id, fecha)
# MAGIC
# MAGIC AFECTADO (H-1b). En lugar de filtrar al MAX(run_ts) global, se selecciona para
# MAGIC cada par (zona_id, fecha_forecast) el run mas reciente que tenga dato para esa
# MAGIC fecha. Asi el dia de hoy usa el run de ayer y manana+ usa el de hoy.

# COMMAND ----------

def seleccionar_mejor_run_por_fecha(df_copernicus):
    """
    Selecciona para cada (zona_id, fecha_forecast) el run mas reciente disponible.

    Resuelve H-1b: Copernicus tiene lag de 1 dia. El run de hoy no tiene datos
    de hoy, pero el run de ayer si los tiene. Esta funcion garantiza que cada
    fecha del Gold window use el run mas reciente que realmente tenga dato
    para esa combinacion de zona y fecha.

    Args:
        df_copernicus: DataFrame de Silver fact_copernicus_forecast filtrado
                       por lookback de al menos 3 dias.

    Returns:
        DataFrame con una sola fila por (zona_id, fecha_cop), correspondiente
        al run mas reciente disponible para esa combinacion.
    """
    df_con_fecha = df_copernicus.withColumn(
        "fecha_cop", F.to_date("timestamp_forecast")
    )

    w_mejor_run = (
        Window
        .partitionBy("zona_id", "fecha_cop")
        .orderBy(F.desc("run_ts"))
    )

    return (
        df_con_fecha
        .withColumn("_rn", F.row_number().over(w_mejor_run))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


df_cp_mejor = seleccionar_mejor_run_por_fecha(df_cp_fix)

print("Copernicus: run usado por zona x fecha")
df_cp_mejor \
    .groupBy("zona_id", "fecha_cop") \
    .agg(F.max("run_ts").alias("run_ts_usado")) \
    .orderBy("zona_id", "fecha_cop") \
    .show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 Agregar Copernicus por (zona_id, fecha)
# MAGIC
# MAGIC AFECTADO (H-1a y H-1b). Copernicus se agrega por fecha en lugar de por
# MAGIC timestamp exacto. El resultado es una fila por (zona_id, fecha) que se
# MAGIC unira a Gold por DATE en la seccion 8.4.

# COMMAND ----------

def agregar_copernicus_por_fecha(df_cp_mejor):
    """
    Agrega variables oceanograficas de Copernicus por zona y fecha.

    Dado que Copernicus P1D-m entrega un dato por zona por dia, esta agregacion
    promedia los valores de las distintas celdas de la grilla dentro del bounding
    box de cada zona. El resultado se une a Gold por (zona_id, DATE) para propagar
    el dato diario a todas las horas del dia.

    Args:
        df_cp_mejor: DataFrame resultado de seleccionar_mejor_run_por_fecha,
                     con columna fecha_cop de tipo DATE.

    Returns:
        DataFrame con una fila por (zona_id, fecha_cop) y columnas agregadas
        de temperatura, salinidad, corrientes, conteo de grillas y run_ts.
    """
    return (
        df_cp_mejor
        .groupBy("zona_id", "fecha_cop")
        .agg(
            F.avg("temperatura_mar_c").alias("temperatura_mar_c"),
            F.avg("salinidad_psu").alias("salinidad_psu"),
            F.avg("corriente_u_ms").alias("corriente_u_ms"),
            F.avg("corriente_v_ms").alias("corriente_v_ms"),
            F.count("*").alias("copernicus_n_grillas"),
            F.max("run_ts").alias("copernicus_run_ts"),
        )
    )


cp_zone_by_date = agregar_copernicus_por_fecha(df_cp_mejor)

print(f"cp_zone_by_date: {cp_zone_by_date.count()} filas (una por zona x dia)")
cp_zone_by_date.orderBy("zona_id", "fecha_cop").show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 Construir base zona/hora sin Copernicus
# MAGIC
# MAGIC AFECTADO (H-1a). Copernicus se elimina de la union base. Incluirlo generaba
# MAGIC filas fantasma a las 00:00 UTC que no corresponden a la resolucion horaria
# MAGIC del forecast. La base ahora solo usa StormGlass y Marea para definir
# MAGIC el universo de timestamps a procesar.

# COMMAND ----------

base_fix = (
    sg_zone.select("zona_id", "timestamp_forecast")
    .unionByName(ma_zone.select("zona_id", "timestamp_forecast"))
    .dropDuplicates(["zona_id", "timestamp_forecast"])
    .where(
        (F.col("timestamp_forecast") >= F.current_timestamp()) &
        (F.col("timestamp_forecast") < F.expr(f"current_timestamp() + INTERVAL {HOURS_FORWARD} HOURS"))
    )
)

print(f"Base fix: {base_fix.count()} filas en ventana de {HOURS_FORWARD}h")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.5 JOIN Copernicus por fecha
# MAGIC
# MAGIC AFECTADO (H-1a). StormGlass y Marea se unen por (zona_id, timestamp_forecast)
# MAGIC como antes. Copernicus se une por (zona_id, DATE(timestamp_forecast)) para que
# MAGIC el unico dato diario se propague a las 24 horas de ese dia en Gold.

# COMMAND ----------

base_con_fecha = base_fix.withColumn(
    "fecha_join", F.to_date("timestamp_forecast")
)

df_gold_fix = (
    base_con_fecha
    .join(sg_zone,  ["zona_id", "timestamp_forecast"], "left")
    .join(ma_zone,  ["zona_id", "timestamp_forecast"], "left")
    .join(
        cp_zone_by_date.withColumnRenamed("fecha_cop", "fecha_join"),
        on=["zona_id", "fecha_join"],
        how="left"
    )
    .drop("fecha_join")
    .withColumn("updated_at", F.current_timestamp())
)

print(f"Gold fix: {df_gold_fix.count()} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.6 Recalculo de precipitation_3h_mm
# MAGIC
# MAGIC No afectado por el fix en su logica. Se recalcula sobre df_gold_fix para
# MAGIC que el acumulado 3h quede correcto en el DataFrame que se enviara al MERGE.

# COMMAND ----------

w_3h_fix = (
    Window
    .partitionBy("zona_id")
    .orderBy("timestamp_forecast")
    .rowsBetween(0, 2)
)

df_gold_fix = df_gold_fix.withColumn(
    "precipitation_3h_mm",
    F.round(
        F.sum(F.coalesce(F.col("precipitation_mm"), F.lit(0.0))).over(w_3h_fix),
        1
    )
)

print("precipitation_3h_mm recalculado sobre df_gold_fix")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.7 MERGE con logica corregida
# MAGIC
# MAGIC AFECTADO (H-1a y H-1b). Sobreescribe el MERGE de la seccion 5 con los datos
# MAGIC correctos de Copernicus. Despues de este MERGE todas las horas del dia tendran
# MAGIC salinidad, temperatura del mar y corrientes siempre que el run del dia anterior
# MAGIC este disponible en Silver.

# COMMAND ----------

n_filas_fix = df_gold_fix.count()
print(f"Filas a escribir: {n_filas_fix}")

df_gold_fix.createOrReplaceTempView("tmp_gold_fix")

spark.sql(f"""
MERGE INTO {GOLD}.forecast_zona_hora AS t
USING tmp_gold_fix AS s
ON  t.zona_id            = s.zona_id
AND t.timestamp_forecast = s.timestamp_forecast
WHEN MATCHED     THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print(f"MERGE fix completado: {n_filas_fix} filas en {GOLD}.forecast_zona_hora")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Post-check Fix H-1a y H-1b
# MAGIC
# MAGIC AFECTADO. Verifica que Copernicus cubre todas las horas del dia y no solo
# MAGIC las 00:00. El porcentaje esperado es 90% si GM01 sigue sin datos oceanicos
# MAGIC (9 de 10 zonas), o 100% si el problema de bbox de GM01 tambien fue resuelto.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cobertura Copernicus por fecha y hora despues del fix
# MAGIC -- Esperado: pct_salinidad = 100% en todas las horas (o 90% si GM01 sin datos)
# MAGIC SELECT
# MAGIC   DATE(timestamp_forecast)                           AS fecha,
# MAGIC   HOUR(timestamp_forecast)                           AS hora_utc,
# MAGIC   COUNT(*)                                            AS total_filas,
# MAGIC   COUNT(salinidad_psu)                                AS con_salinidad,
# MAGIC   ROUND(COUNT(salinidad_psu) * 100.0 / COUNT(*), 1)  AS pct_salinidad
# MAGIC FROM adb_ecoazul.gold.forecast_zona_hora
# MAGIC WHERE DATE(timestamp_forecast) BETWEEN CURRENT_DATE AND CURRENT_DATE + 3
# MAGIC GROUP BY fecha, hora_utc
# MAGIC ORDER BY fecha, hora_utc
# MAGIC LIMIT 30;

# COMMAND ----------

# COMMAND ----------