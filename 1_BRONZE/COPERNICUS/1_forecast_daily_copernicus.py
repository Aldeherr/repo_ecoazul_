# Databricks notebook source
# MAGIC %md
# MAGIC # FORECAST

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONFIGURACION INICIAL

# COMMAND ----------

import os
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import xarray as xr
from pyspark.sql import functions as F


SCOPE = "keyvault-scope"
STORAGE_ACCOUNT = "dataecoazul"

storage_key = dbutils.secrets.get(scope=SCOPE, key="storage-key")
cop_user = dbutils.secrets.get(scope=SCOPE, key="copernicus-user")
cop_pass = dbutils.secrets.get(scope=SCOPE, key="copernicus-pass")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    storage_key
)

os.environ["COPERNICUSMARINE_SERVICE_USERNAME"] = cop_user
os.environ["COPERNICUSMARINE_SERVICE_PASSWORD"] = cop_pass

def get_path(container, path=""):
    """Construye la URL WASBS para acceder a Azure Blob Storage.

    Args:
        container: Nombre del contenedor de Azure Blob.
        path: Ruta dentro del contenedor (opcional). El slash inicial se elimina.

    Returns:
        URL completa en formato wasbs://<container>@<account>.blob.core.windows.net[/path].
    """
    base = f"wasbs://{container}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    return f"{base}/{path.lstrip('/')}" if path else base

OUTPUT_DIR = Path("/tmp/datos_copernicus_forecast")
OUTPUT_DIR.mkdir(exist_ok=True)


import copernicusmarine

# COMMAND ----------

puntos_pesca = [
    {
        "id": "BT01",
        "nombre": "Bocas del Toro (continental)",
        "lat": 9.3985776,
        "lng": -82.1923224,
        "provincias": "Bocas del Toro",
    },
    {
        "id": "CO01",
        "nombre": "Costa Abajo Colón",
        "lat": 9.2360403,
        "lng": -80.1992382,
        "provincias": "Colón",
    },
    {
        "id": "GG01",
        "nombre": "Golfo de Panamá – líneas costeras",
        "lat": 8.8,
        "lng": -79.7,
        "provincias": "Panamá",
    },
    {
        "id": "PVX01",
        "nombre": "Pixvae",
        "lat": 7.8351394,
        "lng": -81.6408541,
        "provincias": "Veraguas",
    },
    {
        "id": "BH01",
        "nombre": "Bahía Honda",
        "lat": 7.6762836,
        "lng": -81.47205,
        "provincias": "Veraguas",
    },
    {
        "id": "GM01",
        "nombre": "Golfo Montijo",
        "lat": 7.6932686,
        "lng": -81.1180117,
        "provincias": "Veraguas",
    },
    {
        "id": "PD01",
        "nombre": "Pedasí",
        "lat": 7.3992057,
        "lng": -80.1020331,
        "provincias": "Los Santos",
    },
    {
        "id": "GP01",
        "nombre": "Bahía de Parita",
        "lat": 8.1630421,
        "lng": -80.3990094,
        "provincias": "Herrera",
    },
    {
        "id": "PCH01",
        "nombre": "Punta Chame",
        "lat": 8.5706506,
        "lng": -79.7315935,
        "provincias": "Pma. Oeste",
    },
    {
        "id": "GCH01",
        "nombre": "Golfo Chiriquí",
        "lat": 8.2245103,
        "lng": -82.4499123,
        "provincias": "Chiriquí",
    }
]

# Crear zonas con bounding box
OFFSET = 0.25  # ±27.5 km (antes: 0.5 = ±55 km)

zonas = {}
for p in puntos_pesca:

    if p['id'] == 'GM01':

        zonas[p['id']] = {
            'nombre': p['nombre'],
            'lat_min': 7.35,
            'lat_max': 7.95,
            'lon_min': -81.35,   # FIX H-4: era lng_min → lon_min (KeyError corregido)
            'lon_max': -80.97,   # FIX H-4: era lng_max → lon_max
            'lat_centro': p['lat'],
            'lng_centro': p['lng']
        }
        
    else:
        # OFFSET estándar para otras zonas
        zonas[p['id']] = {
            'nombre': p['nombre'],
            'lat_min': p['lat'] - OFFSET,
            'lat_max': p['lat'] + OFFSET,
            'lon_min': p['lng'] - OFFSET,
            'lon_max': p['lng'] + OFFSET,
            'lat_centro': p['lat'],
            'lng_centro': p['lng']
        } 


# COMMAND ----------

# MAGIC %md
# MAGIC ## CONFIGURACION FORECAST

# COMMAND ----------

hoy = datetime.now()
dentro_72h = hoy + timedelta(days=3)  # FIX: era days=2 (48h) → days=3 (72h) para cubrir forecast_zona_hora completo

# Forecasts datasets (SIN depth, es superficie por defecto)
CONFIG_FORECAST_TEMP = {
    'dataset_id': 'cmems_mod_glo_phy-thetao_anfc_0.083deg_P1D-m',
    'variables': ['thetao'],
    'start': hoy.isoformat(),
    'end': dentro_72h.isoformat()
}

CONFIG_FORECAST_SAL = {
    'dataset_id': 'cmems_mod_glo_phy-so_anfc_0.083deg_P1D-m',
    'variables': ['so'],
    'start': hoy.isoformat(),
    'end': dentro_72h.isoformat()
}

CONFIG_FORECAST_CURR = {
    'dataset_id': 'cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m',
    'variables': ['uo', 'vo'],
    'start': hoy.isoformat(),
    'end': dentro_72h.isoformat()
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## FUNCION DE DESCARGA

# COMMAND ----------

def descargar_zona_forecast(zona_id, zona_info, config, tipo):
    """
    Descarga forecasts (datasets de superficie, SIN especificar depth)
    """
    print(f"  {zona_id}: {zona_info['nombre']}... ", end="")
    
    start_str = config['start'].replace('-','').replace(':','').replace('T','').replace('.','')[:8]
    end_str = config['end'].replace('-','').replace(':','').replace('T','').replace('.','')[:8]
    
    filename = f"{zona_id}_{tipo}_{start_str}_{end_str}.nc"
    filepath = OUTPUT_DIR / filename
    
    if filepath.exists():
        print("cache")
    else:
        try:
            # Datasets forecast son de SUPERFICIE
            # NO tienen dimension depth, no especificar minimum/maximum_depth
            
            copernicusmarine.subset(
                dataset_id=config['dataset_id'],
                variables=config['variables'],
                minimum_longitude=zona_info['lon_min'],
                maximum_longitude=zona_info['lon_max'],
                minimum_latitude=zona_info['lat_min'],
                maximum_latitude=zona_info['lat_max'],
                start_datetime=config['start'],
                end_datetime=config['end'],
                # SIN depth (estos datasets son superficie)
                output_filename=str(filepath),
            )
            print("descargado")
        except Exception as e:
            error_msg = str(e)[:50]
            print(f"{error_msg}")
            return None
    
    try:
        ds = xr.open_dataset(filepath)
        
        # Filtrar superficie si tiene depth
        if 'depth' in ds.coords:
            superficie = ds.isel(depth=0)
        else:
            superficie = ds
        
        df = superficie.to_dataframe().reset_index()
        
        registros_iniciales = len(df)
        
        # LIMPIEZA TIERRA/COSTA (igual que Monthly)
        variables_oceano = ['thetao', 'so', 'uo', 'vo']
        vars_existentes = [v for v in variables_oceano if v in df.columns]
        
        if len(vars_existentes) > 0:
            mask_con_datos = df[vars_existentes].notna().any(axis=1)
            df = df[mask_con_datos]
        
        eliminados = registros_iniciales - len(df)
        if eliminados > 0:
            print(f"[tierra: -{eliminados}] ", end="")
        
        # Metadatos
        df['zona_id'] = zona_id
        df['nombre_zona'] = zona_info['nombre']
        df['tipo_dato'] = tipo
        
        ds.close()
        
        if len(df) == 0:
            print(f"sin datos")
            return None

        print(f"OK {len(df):,} registros")
        return df
        
    except Exception as e:
        print(f"  Error: {str(e)[:30]}")
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## DESCARGAR LOS 3 FORECASTS

# COMMAND ----------

#FORECAST TEMPERATURA

dataframes_forecast_temp = []
for zona_id, zona_info in zonas.items():
    df = descargar_zona_forecast(zona_id, zona_info, CONFIG_FORECAST_TEMP, "forecast_temp")
    if df is not None:
        dataframes_forecast_temp.append(df)

# COMMAND ----------

# FORECAST SALINIDAD
dataframes_forecast_sal = []
for zona_id, zona_info in zonas.items():
    df = descargar_zona_forecast(zona_id, zona_info, CONFIG_FORECAST_SAL, "forecast_sal")
    if df is not None:
        dataframes_forecast_sal.append(df)

# COMMAND ----------

# FORECAST CORRIENTES
dataframes_forecast_curr = []
for zona_id, zona_info in zonas.items():
    df = descargar_zona_forecast(zona_id, zona_info, CONFIG_FORECAST_CURR, "forecast_curr")
    if df is not None:
        dataframes_forecast_curr.append(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONSOLIDAR

# COMMAND ----------

todos = []
todos.extend(dataframes_forecast_temp)
todos.extend(dataframes_forecast_sal)
todos.extend(dataframes_forecast_curr)

if len(todos) == 0:
    print("X No hay forecasts para consolidar")
    dbutils.notebook.exit({"status": "error", "message": "No forecasts"})

df_consolidado = pd.concat(todos, ignore_index=True)

# Normalizar
columnas_requeridas = ['time', 'depth', 'latitude', 'longitude', 
                       'thetao', 'so', 'uo', 'vo', 
                       'zona_id', 'nombre_zona', 'tipo_dato']

for col in columnas_requeridas:
    if col not in df_consolidado.columns:
        df_consolidado[col] = pd.NA

df_consolidado = df_consolidado[columnas_requeridas]

# COMMAND ----------

# MAGIC %md
# MAGIC ## GUARDAR EN BRONZE

# COMMAND ----------

df_spark = spark.createDataFrame(df_consolidado)
df_spark = df_spark.withColumn("timestamp_ingesta", F.current_timestamp())

today = datetime.now()
year, month, day = today.year, f"{today.month:02d}", f"{today.day:02d}"

filename = f"copernicus_forecast_72h_{hoy.strftime('%Y%m%d')}.parquet"  # FIX: 48h → 72h

output_path = get_path(
    "bronze",
    f"copernicus/year={year}/month={month}/forecast/day={day}/{filename}"
)

# Escribir
df_spark.coalesce(1).write.mode("overwrite").parquet(output_path)
output_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## LIMPIAR Y RETORNAR

# COMMAND ----------

import shutil
import json

if OUTPUT_DIR.exists():
    shutil.rmtree(OUTPUT_DIR)

result = {
    "status": "success",
    "tipo": "forecast_72h",
    "fecha_ejecucion": datetime.now().isoformat(),
    "ventana_inicio": hoy.strftime('%Y-%m-%d'),
    "ventana_fin": dentro_72h.strftime('%Y-%m-%d'),
    "zonas_procesadas": len(zonas),
    "total_registros": len(df_consolidado),
    "output_path": output_path
}

print(" FORECAST COMPLETADO")
for key, value in result.items():
    print(f"  {key}: {value}")
print(f"{'='*60}")

dbutils.notebook.exit(json.dumps(result))