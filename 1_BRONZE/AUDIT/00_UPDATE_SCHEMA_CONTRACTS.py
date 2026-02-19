# Databricks notebook source
# MAGIC %md
# MAGIC # ACTUALIZAR SCHEMA CONTRACTS
# MAGIC
# MAGIC Propósito: Regenerar fingerprints de esquema después de cambios en Bronze
# MAGIC
# MAGIC Basado en: 01_data_quality_bronze.py (usa mismos métodos de acceso)
# MAGIC
# MAGIC Estructura Bronze:
# MAGIC - StormGlass: stormglass/year=YYYY/month=MM/forecast/day=DD/stormglass_forecast_48h*.parquet
# MAGIC - Marea:      marea/year=YYYY/month=MM/forecast/day=DD/marea_forecast_*.parquet
# MAGIC - Copernicus: copernicus/year=YYYY/month=MM/forecast/day=DD/copernicus_forecast_72h*.parquet
# MAGIC
# MAGIC Tiempo estimado: menos de 3 minutos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuracion Inicial

# COMMAND ----------

from databricks.sdk.runtime import dbutils
from pyspark.sql import functions as F
import json, hashlib
from datetime import datetime

# ============================================================
# MISMA CONFIGURACION que 01_data_quality_bronze.py
# ============================================================
SCOPE = "keyvault-scope"
STORAGE_ACCOUNT = "dataecoazul"
CONTAINER = "bronze"

# Autenticacion con Azure Storage
storage_key = dbutils.secrets.get(scope=SCOPE, key="storage-key")
spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", storage_key)

print("Autenticacion configurada")
print(f"  Storage Account: {STORAGE_ACCOUNT}")
print(f"  Container: {CONTAINER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Funciones Helper

# COMMAND ----------

def get_path(container, path=""):
    """Construye path completo para Azure Blob Storage"""
    base = f"wasbs://{container}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    return f"{base}/{path.lstrip('/')}" if path else base

def lsR_parquet(path):
    """Busca recursivamente todos los parquets en un path"""
    out = []
    try:
        for fi in dbutils.fs.ls(path):
            if fi.isFile():
                if fi.path.lower().endswith(".parquet"):
                    out.append(fi.path)
            else:
                out.extend(lsR_parquet(fi.path))
    except Exception as e:
        print(f"  Warning: No se pudo listar {path}: {e}")
    return out

def schema_fingerprint(schema) -> str:
    """Calcula SHA256 del esquema JSON (igual que data_quality)"""
    j = json.dumps(schema.jsonValue(), sort_keys=True)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()

# Paths de contratos (igual que data_quality_bronze.py)
CONTRACT_FP = {
    "marea":       "/Volumes/adb_ecoazul/config/contracts/marea/schema_fp_v1.txt",
    "stormglass":  "/Volumes/adb_ecoazul/config/contracts/stormglass/schema_fp_v1.txt",
    "copernicus":  "/Volumes/adb_ecoazul/config/contracts/copernicus/schema_fp_v1.txt"
}

print("Funciones helper cargadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Buscar Parquets Mas Recientes (Estructura year/month/forecast/day)

# COMMAND ----------

def find_latest_parquet_partitioned(source: str) -> tuple:
    """
    Encuentra el parquet mas reciente de un source con estructura particionada.
    Estructura: {source}/year=YYYY/month=MM/forecast/day=DD/*.parquet

    Retorna: (path_completo, fecha_YYYYMMDD, lista_parquets)
    """
    try:
        base = get_path(CONTAINER, f"{source}/")
        print(f"  Buscando en: {base}")

        # Listar anios (year=*)
        years = []
        try:
            for folder in dbutils.fs.ls(base):
                if "year=" in folder.name:
                    year = folder.name.split("year=")[1].rstrip("/")
                    years.append((year, folder.path))
        except Exception as e:
            print(f"  Warning: Error listando anios: {e}")
            return None, None, []

        if not years:
            print(f"  Warning: No se encontraron carpetas year=* en {base}")
            return None, None, []

        years.sort(reverse=True, key=lambda x: x[0])
        print(f"  Anios encontrados: {[y[0] for y in years]}")

        all_dates = []

        for year, year_path in years:
            try:
                months = []
                for folder in dbutils.fs.ls(year_path):
                    if "month=" in folder.name:
                        month = folder.name.split("month=")[1].rstrip("/")
                        months.append((month, folder.path))

                months.sort(reverse=True, key=lambda x: x[0])

                for month, month_path in months:
                    forecast_path = month_path.rstrip("/") + "/forecast/"
                    try:
                        days = []
                        for folder in dbutils.fs.ls(forecast_path):
                            if "day=" in folder.name:
                                day = folder.name.split("day=")[1].rstrip("/")
                                days.append((day, folder.path))

                        days.sort(reverse=True, key=lambda x: x[0])

                        for day, day_path in days:
                            date_str = f"{year}{month.zfill(2)}{day.zfill(2)}"
                            all_dates.append((date_str, day_path, year, month, day))
                    except Exception:
                        continue
            except Exception as e:
                print(f"  Warning: Error procesando year={year}: {e}")
                continue

        if not all_dates:
            print(f"  Warning: No se encontraron carpetas day=* en ninguna particion")
            return None, None, []

        all_dates.sort(reverse=True, key=lambda x: x[0])
        latest_date_str, latest_path, year, month, day = all_dates[0]

        print(f"  Particion mas reciente: year={year}/month={month}/day={day}")

        parquets = lsR_parquet(latest_path)

        if not parquets:
            print(f"  Warning: No se encontraron parquets en {latest_path}")
            return latest_path, latest_date_str, []

        print(f"  Parquets encontrados: {len(parquets)}")
        return latest_path, latest_date_str, parquets

    except Exception as e:
        print(f"ERROR buscando parquets de {source}: {e}")
        import traceback
        traceback.print_exc()
        return None, None, []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. STORMGLASS - Actualizar Schema Contract

# COMMAND ----------

print("=" * 70)
print("STORMGLASS - Actualizando Schema Contract")
print("=" * 70)

sg_path, sg_date, sg_parquets = find_latest_parquet_partitioned("stormglass")

if sg_path and sg_parquets:
    print(f"\nDatos encontrados:")
    print(f"  Fecha: {sg_date}")
    print(f"  Path: {sg_path}")
    print(f"  Parquets: {len(sg_parquets)}")

    first_parquet = sg_parquets[0]
    parquet_name = first_parquet.split('/')[-1]
    print(f"  Leyendo: {parquet_name}")

    try:
        df_sg = spark.read.parquet(first_parquet)

        print(f"\nParquet leido exitosamente")
        print(f"  Filas: {df_sg.count()}")
        print(f"  Columnas ({len(df_sg.columns)}): {', '.join(df_sg.columns)}")

        # Verificar que tiene raw_data con hours
        print(f"\nVerificando estructura raw_data.hours:")
        sample_row = df_sg.select("raw_data").first()
        has_visibility = False

        if sample_row:
            sample_dict = sample_row.asDict()
            raw_data = sample_dict.get("raw_data")

            if raw_data and isinstance(raw_data, dict):
                hours = raw_data.get("hours", [])
                if hours and len(hours) > 0:
                    first_hour = hours[0]
                    print(f"  raw_data.hours existe")
                    print(f"  Total horas en muestra: {len(hours)}")

                    if isinstance(first_hour, dict):
                        print(f"  Campos en hours[0]: {', '.join(sorted(first_hour.keys()))}")

                        if "visibility" in first_hour:
                            print(f"\n  Campo 'visibility' PRESENTE en hours[0]")
                            print(f"    Estructura: {first_hour['visibility']}")
                            has_visibility = True
                        else:
                            print(f"\n  WARNING: Campo 'visibility' NO encontrado en hours[0]")
                            print(f"  Campos disponibles:")
                            for key in sorted(first_hour.keys()):
                                print(f"    - {key}")
                    else:
                        print(f"  Warning: first_hour no es dict, es: {type(first_hour)}")
                else:
                    print(f"  Warning: raw_data.hours esta vacio o no existe")
            else:
                print(f"  Warning: raw_data no es dict, es: {type(raw_data)}")
        else:
            print(f"  Warning: No se pudo leer raw_data o esta vacio")

        # Calcula nuevo fingerprint
        fp_sg_new = schema_fingerprint(df_sg.schema)
        print(f"\nNuevo Fingerprint (SHA256):")
        print(f"  {fp_sg_new}")

        # Lee el fingerprint actual almacenado
        fp_sg_old = None
        try:
            fp_sg_old = dbutils.fs.head(CONTRACT_FP["stormglass"]).strip()
            print(f"\nFingerprint actual en Volumes:")
            print(f"  {fp_sg_old}")

            if fp_sg_old == fp_sg_new:
                print(f"\n  INFO: Fingerprints COINCIDEN (no hay cambio de esquema)")
                print(f"  El contrato ya esta actualizado")
            else:
                print(f"\n  WARNING: Fingerprints DIFERENTES (esquema cambio)")
                if has_visibility:
                    print(f"  Esto es ESPERADO - visibility fue agregado")
                else:
                    print(f"  Visibility NO esta en datos pero esquema cambio por otra razon")
        except Exception as e:
            print(f"\nNo se pudo leer fingerprint anterior: {e}")
            print(f"  Primera vez creando contrato")

        # Actualiza contrato en Volumes
        print(f"\nActualizando contrato en Volumes...")
        dbutils.fs.put(CONTRACT_FP["stormglass"], fp_sg_new, overwrite=True)

        # Verifica que se escribio
        stored_fp = dbutils.fs.head(CONTRACT_FP["stormglass"]).strip()
        if stored_fp == fp_sg_new:
            print(f"OK - Contrato actualizado exitosamente:")
            print(f"  Path: {CONTRACT_FP['stormglass']}")
            print(f"  Fingerprint: {fp_sg_new[:32]}...")
            sg_result = "SUCCESS"
            sg_error = None
        else:
            raise Exception("ERROR: Fingerprint no coincide despues de guardar")

    except Exception as e:
        print(f"\nERROR procesando StormGlass: {e}")
        import traceback
        traceback.print_exc()
        sg_result = "FAILED"
        sg_error = str(e)
        fp_sg_new = None
        has_visibility = False
else:
    print("\nERROR: No se encontraron parquets de StormGlass en Bronze")
    print(f"  Verificar estructura: {get_path(CONTAINER, 'stormglass/year=YYYY/month=MM/forecast/day=DD/')}")
    sg_result = "NOT_FOUND"
    sg_error = "No parquets found"
    fp_sg_new = None
    sg_date = None
    has_visibility = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. MAREA - Actualizar Schema Contract

# COMMAND ----------

print("\n" + "=" * 70)
print("MAREA - Actualizando Schema Contract")
print("=" * 70)

marea_path, marea_date, marea_parquets = find_latest_parquet_partitioned("marea")

if marea_path and marea_parquets:
    print(f"\nDatos encontrados:")
    print(f"  Fecha: {marea_date}")
    print(f"  Path: {marea_path}")
    print(f"  Parquets: {len(marea_parquets)}")

    first_parquet = marea_parquets[0]
    parquet_name = first_parquet.split('/')[-1]
    print(f"  Leyendo: {parquet_name}")

    try:
        df_marea = spark.read.parquet(first_parquet)

        print(f"\nParquet leido exitosamente")
        print(f"  Filas: {df_marea.count()}")
        print(f"  Columnas: {len(df_marea.columns)}")

        fp_marea_new = schema_fingerprint(df_marea.schema)
        print(f"\nNuevo Fingerprint (SHA256):")
        print(f"  {fp_marea_new}")

        fp_marea_old = None
        try:
            fp_marea_old = dbutils.fs.head(CONTRACT_FP["marea"]).strip()
            print(f"\nFingerprint actual en Volumes:")
            print(f"  {fp_marea_old}")

            if fp_marea_old == fp_marea_new:
                print(f"\n  INFO: Fingerprints COINCIDEN")
            else:
                print(f"\n  WARNING: Fingerprints DIFERENTES")
        except Exception as e:
            print(f"\nNo se pudo leer fingerprint anterior: {e}")

        print(f"\nActualizando contrato en Volumes...")
        dbutils.fs.put(CONTRACT_FP["marea"], fp_marea_new, overwrite=True)

        stored_fp_marea = dbutils.fs.head(CONTRACT_FP["marea"]).strip()
        if stored_fp_marea == fp_marea_new:
            print(f"OK - Contrato actualizado exitosamente:")
            print(f"  Path: {CONTRACT_FP['marea']}")
            print(f"  Fingerprint: {fp_marea_new[:32]}...")
            marea_result = "SUCCESS"
            marea_error = None
        else:
            raise Exception("ERROR: Fingerprint no coincide despues de guardar")

    except Exception as e:
        print(f"\nERROR procesando Marea: {e}")
        import traceback
        traceback.print_exc()
        marea_result = "FAILED"
        marea_error = str(e)
        fp_marea_new = None
else:
    print("\nERROR: No se encontraron parquets de Marea en Bronze")
    print(f"  Verificar estructura: {get_path(CONTAINER, 'marea/year=YYYY/month=MM/forecast/day=DD/')}")
    marea_result = "NOT_FOUND"
    marea_error = "No parquets found"
    fp_marea_new = None
    marea_date = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. COPERNICUS - Actualizar Schema Contract

# COMMAND ----------

print("\n" + "=" * 70)
print("COPERNICUS - Actualizando Schema Contract")
print("=" * 70)

cop_path, cop_date, cop_parquets = find_latest_parquet_partitioned("copernicus")

if cop_path and cop_parquets:
    print(f"\nDatos encontrados:")
    print(f"  Fecha: {cop_date}")
    print(f"  Path: {cop_path}")
    print(f"  Parquets: {len(cop_parquets)}")

    first_parquet = cop_parquets[0]
    parquet_name = first_parquet.split('/')[-1]
    print(f"  Leyendo: {parquet_name}")

    try:
        df_cop = spark.read.parquet(first_parquet)

        print(f"\nParquet leido exitosamente")
        print(f"  Filas: {df_cop.count()}")
        print(f"  Columnas ({len(df_cop.columns)}): {', '.join(df_cop.columns)}")

        # Calcula nuevo fingerprint
        fp_cop_new = schema_fingerprint(df_cop.schema)
        print(f"\nNuevo Fingerprint (SHA256):")
        print(f"  {fp_cop_new}")

        # Lee el fingerprint actual almacenado
        fp_cop_old = None
        try:
            fp_cop_old = dbutils.fs.head(CONTRACT_FP["copernicus"]).strip()
            print(f"\nFingerprint actual en Volumes:")
            print(f"  {fp_cop_old}")

            if fp_cop_old == fp_cop_new:
                print(f"\n  INFO: Fingerprints COINCIDEN (no hay cambio de esquema)")
                print(f"  El contrato ya esta actualizado")
            else:
                print(f"\n  WARNING: Fingerprints DIFERENTES (esquema cambio)")
                print(f"  Columnas actuales: {df_cop.columns}")
        except Exception as e:
            print(f"\nNo se pudo leer fingerprint anterior: {e}")
            print(f"  Primera vez creando contrato")

        # Actualiza contrato en Volumes
        print(f"\nActualizando contrato en Volumes...")
        dbutils.fs.put(CONTRACT_FP["copernicus"], fp_cop_new, overwrite=True)

        # Verifica que se escribio
        stored_fp = dbutils.fs.head(CONTRACT_FP["copernicus"]).strip()
        if stored_fp == fp_cop_new:
            print(f"OK - Contrato actualizado exitosamente:")
            print(f"  Path: {CONTRACT_FP['copernicus']}")
            print(f"  Fingerprint: {fp_cop_new[:32]}...")
            cop_result = "SUCCESS"
            cop_error = None
        else:
            raise Exception("ERROR: Fingerprint no coincide despues de guardar")

    except Exception as e:
        print(f"\nERROR procesando Copernicus: {e}")
        import traceback
        traceback.print_exc()
        cop_result = "FAILED"
        cop_error = str(e)
        fp_cop_new = None
else:
    print("\nERROR: No se encontraron parquets de Copernicus en Bronze")
    print(f"  Verificar estructura: {get_path(CONTAINER, 'copernicus/year=YYYY/month=MM/forecast/day=DD/')}")
    cop_result = "NOT_FOUND"
    cop_error = "No parquets found"
    fp_cop_new = None
    cop_date = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen Final

# COMMAND ----------

print("\n" + "=" * 70)
print("RESUMEN FINAL")
print("=" * 70)

results = {
    "timestamp": datetime.utcnow().isoformat(),
    "stormglass": {
        "status": sg_result,
        "date": sg_date,
        "fingerprint": fp_sg_new,
        "has_visibility": has_visibility,
        "error": sg_error
    },
    "marea": {
        "status": marea_result,
        "date": marea_date,
        "fingerprint": fp_marea_new,
        "error": marea_error
    },
    "copernicus": {
        "status": cop_result,
        "date": cop_date,
        "fingerprint": fp_cop_new,
        "error": cop_error
    }
}

print("\nStormGlass:")
print(f"  Status: {sg_result}")
print(f"  Fecha: {sg_date}")
if sg_result == "SUCCESS":
    print(f"  Contrato actualizado en Volumes")
    print(f"  Fingerprint: {fp_sg_new[:32]}...")
    if has_visibility:
        print(f"  VISIBILITY PRESENTE en datos - OK")
    else:
        print(f"  WARNING: VISIBILITY NO encontrado en datos")
        print(f"  Accion: Verificar config_apis.json y redeploy Azure Functions")
elif sg_result == "FAILED":
    print(f"  ERROR: {sg_error}")
elif sg_result == "NOT_FOUND":
    print(f"  WARNING: No se encontraron parquets en Bronze")

print("\nMarea:")
print(f"  Status: {marea_result}")
print(f"  Fecha: {marea_date}")
if marea_result == "SUCCESS":
    print(f"  Contrato actualizado en Volumes")
    print(f"  Fingerprint: {fp_marea_new[:32]}...")
elif marea_result == "FAILED":
    print(f"  ERROR: {marea_error}")
elif marea_result == "NOT_FOUND":
    print(f"  WARNING: No se encontraron parquets en Bronze")

print("\nCopernicus:")
print(f"  Status: {cop_result}")
print(f"  Fecha: {cop_date}")
if cop_result == "SUCCESS":
    print(f"  Contrato actualizado en Volumes")
    print(f"  Fingerprint: {fp_cop_new[:32]}...")
elif cop_result == "FAILED":
    print(f"  ERROR: {cop_error}")
elif cop_result == "NOT_FOUND":
    print(f"  WARNING: No se encontraron parquets en Bronze")

# Guardar log
try:
    log_path = f"/Volumes/adb_ecoazul/config/logs/schema_contract_update_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    dbutils.fs.put(log_path, json.dumps(results, indent=2), overwrite=True)
    print(f"\nLog guardado: {log_path}")
except Exception as e:
    print(f"\nWarning: No se pudo guardar log: {e}")

print("\n" + "=" * 70)
all_ok = sg_result == "SUCCESS" and marea_result == "SUCCESS" and cop_result == "SUCCESS"
if all_ok:
    print("CONTRATOS ACTUALIZADOS EXITOSAMENTE (StormGlass + Marea + Copernicus)")
    print("\nProximos pasos:")
    if has_visibility:
        print("  Visibility esta en datos - puedes continuar")
        print("  1. Ejecuta: 04_silver_fact_stormglass.py")
        print("  2. Luego:   01_gold_forecast_zona_hora.py")
        print("  3. Luego:   02_gold_condiciones_actuales_zona.py")
    else:
        print("  WARNING: Visibility NO esta en datos actuales")
        print("  1. Verifica config_apis.json en Storage (debe tener ,visibility)")
        print("  2. Redeploy Azure Functions IngestStormGlass")
        print("  3. Espera proxima ingesta automatica")
        print("  4. Vuelve a ejecutar este notebook")
        print("  5. Luego ejecuta notebooks Silver y Gold")
elif sg_result == "SUCCESS" or marea_result == "SUCCESS" or cop_result == "SUCCESS":
    print("WARNING: ALGUNOS CONTRATOS ACTUALIZADOS - revisar errores arriba")
else:
    print("ERROR: NINGUN CONTRATO ACTUALIZADO")
    print("  Verifica que existan datos en Bronze")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Diagnostico - Estructura de Carpetas

# COMMAND ----------

print("\nEstructura de Bronze:")
print("=" * 70)

for source in ["stormglass", "marea"]:
    print(f"\n{source.upper()}:")
    try:
        base = get_path(CONTAINER, f"{source}/")

        years = []
        for folder in dbutils.fs.ls(base):
            if "year=" in folder.name:
                year = folder.name.split("year=")[1].rstrip("/")
                years.append(year)

        if years:
            years.sort(reverse=True)
            print(f"  Anios disponibles: {', '.join(years)}")

            latest_year = years[0]
            year_path = base + f"year={latest_year}/"

            months = []
            for folder in dbutils.fs.ls(year_path):
                if "month=" in folder.name:
                    month = folder.name.split("month=")[1].rstrip("/")
                    months.append(month)

            if months:
                months.sort(reverse=True)
                print(f"  Meses en {latest_year}: {', '.join(months)}")

                latest_month = months[0]
                forecast_path = year_path + f"month={latest_month}/forecast/"

                try:
                    days = []
                    for folder in dbutils.fs.ls(forecast_path):
                        if "day=" in folder.name:
                            day = folder.name.split("day=")[1].rstrip("/")
                            days.append(day)

                    if days:
                        days.sort(reverse=True)
                        print(f"  Dias en {latest_year}/{latest_month}: {', '.join(days)}")
                        print(f"  Estructura OK: {source}/year={latest_year}/month={latest_month}/forecast/day={days[0]}/")
                except Exception:
                    print(f"  Warning: No existe carpeta forecast/ en month={latest_month}")
        else:
            print(f"  Warning: No se encontraron carpetas year=* en {base}")

    except Exception as e:
        print(f"  ERROR: {e}")
