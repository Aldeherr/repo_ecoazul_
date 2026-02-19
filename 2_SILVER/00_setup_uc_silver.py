# Databricks notebook source
# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN adb_ecoazul;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS adb_ecoazul.silver;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_ecoazul.silver._smoke_test (id INT) USING DELTA;
# MAGIC INSERT INTO adb_ecoazul.silver._smoke_test VALUES (1);
# MAGIC SELECT * FROM adb_ecoazul.silver._smoke_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE adb_ecoazul.silver._perm_test USING DELTA AS SELECT current_timestamp() ts;

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREACION DE TABLAS

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adb_ecoazul;
# MAGIC CREATE SCHEMA IF NOT EXISTS adb_ecoazul.silver;
# MAGIC USE SCHEMA silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adb_ecoazul.silver.dim_zonas (
# MAGIC   zona_id STRING,
# MAGIC   zona_nombre STRING,
# MAGIC   lat_centro DOUBLE,
# MAGIC   lon_centro DOUBLE,
# MAGIC   lat_min DOUBLE,
# MAGIC   lat_max DOUBLE,
# MAGIC   lon_min DOUBLE,
# MAGIC   lon_max DOUBLE,
# MAGIC   fuente_geometria STRING,
# MAGIC   activo BOOLEAN,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
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

# COMMAND ----------


