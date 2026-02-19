# Databricks notebook source


# COMMAND ----------

import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd
import xarray as xr
import json

# Configuración Key Vault
SCOPE = "keyvault-scope"
STORAGE_ACCOUNT = "dataecoazul"

# Obtener secrets desde Key Vault
storage_key = dbutils.secrets.get(scope=SCOPE, key="storage-key")
cop_user = dbutils.secrets.get(scope=SCOPE, key="copernicus-user")
cop_pass = dbutils.secrets.get(scope=SCOPE, key="copernicus-pass")

# Configurar Storage Account
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    storage_key
)

# Configurar COPERNICUS
os.environ["COPERNICUSMARINE_SERVICE_USERNAME"] = cop_user
os.environ["COPERNICUSMARINE_SERVICE_PASSWORD"] = cop_pass

import copernicusmarine
print("... Configurando credenciales de COPERNICUS ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FUNCIONES

# COMMAND ----------

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

# Directorio temporal
OUTPUT_DIR = Path("/tmp/datos_copernicus")
OUTPUT_DIR.mkdir(exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CALCULAR PERIODO

# COMMAND ----------

hoy = datetime.now()
primer_dia_mes_anterior = (hoy.replace(day=1) - timedelta(days=1)).replace(day=1)
ultimo_dia_mes_anterior = hoy.replace(day=1) - timedelta(days=1)

# # Formatear fechas
start_date = primer_dia_mes_anterior.strftime("%Y-%m-%d")
end_date = ultimo_dia_mes_anterior.strftime("%Y-%m-%d")

# primer_dia_mes_anterior = datetime(2025,12,1)
# ultimo_dia_mes_anterior = datetime(2025,12,31)

# start_date = '2025-12-01'
# end_date = '2025-12-31'

print(start_date, end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PUNTO PESCA

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
            'lng_min': -81.35,
            'lng_max': -80.97,
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
# MAGIC ## CONFIGURACION HISTORICO

# COMMAND ----------

CONFIG_HISTORICO = {
    'dataset_id': 'cmems_mod_glo_phy_my_0.083deg_P1D-m',
    'variables': ['thetao', 'so', 'uo', 'vo'],
    'start': start_date,
    'end': end_date
}

print(f"   Dataset: {CONFIG_HISTORICO['dataset_id']}")
print(f"   Variables: {CONFIG_HISTORICO['variables']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FUNCION DE DESCARGA

# COMMAND ----------

def descargar_zona(zona_id, zona_info, config, tipo="historico"):
    """
    Descarga datos históricos para una zona
    """
    print(f"  {zona_id}: {zona_info['nombre']:30} ", end="", flush=True)
    
    # Nombre archivo
    start_str = config['start'].replace('-','')
    end_str = config['end'].replace('-','')
    
    filename = f"{zona_id}_{tipo}_{start_str}_{end_str}.nc"
    filepath = OUTPUT_DIR / filename
    
    # Descargar si no existe
    if filepath.exists():
        print("cache ", end="")
    else:
        try:
            copernicusmarine.subset(
                dataset_id=config['dataset_id'],
                variables=config['variables'],
                minimum_longitude=zona_info['lon_min'],
                maximum_longitude=zona_info['lon_max'],
                minimum_latitude=zona_info['lat_min'],
                maximum_latitude=zona_info['lat_max'],
                start_datetime=config['start'],
                end_datetime=config['end'],
                output_filename=str(filepath),
            )
            print("✓ descargado ", end="")
        except Exception as e:
            print(f"✗ Error: {str(e)[:30]}")
            return None
    
    # Procesar NetCDF
    try:
        ds = xr.open_dataset(filepath)
        
        # Filtrar superficie
        if 'depth' in ds.coords:
            superficie = ds.isel(depth=0)
            depth_value = float(superficie.depth.values)
            print(f"[depth={depth_value:.1f}m] ", end="")
        else:
            superficie = ds
        
        # Convertir a DataFrame
        df = superficie.to_dataframe().reset_index()
        
        registros_iniciales = len(df)
        
        variables_oceano = ['thetao', 'so', 'uo', 'vo']
        vars_existentes = [v for v in variables_oceano if v in df.columns]
        
        if len(vars_existentes) > 0:
            # Eliminar registros donde TODAS las variables son NULL
            # (esto incluye tierra, costa, zonas sin mediciones)
            mask_con_datos = df[vars_existentes].notna().any(axis=1)
            df = df[mask_con_datos]
        
        registros_finales = len(df)
        eliminados = registros_iniciales - registros_finales
        
        if eliminados > 0:
            pct_eliminado = (eliminados / registros_iniciales) * 100
            print(f"[tierra: -{eliminados} ({pct_eliminado:.0f}%)] ", end="")
        
        # Metadatos
        df['zona_id'] = zona_id
        df['nombre_zona'] = zona_info['nombre']
        df['tipo_dato'] = tipo
        
        ds.close()
        
        if len(df) == 0:
            print(f"⚠️  sin datos")
            return None
        
        print(f"✓ {len(df):,} registros")
        return df
        
    except Exception as e:
        print(f"Proceso: {str(e)[:30]}")
        return None

print("Función de descarga lista (con limpieza automática)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DESCARGAR HISTORICOS

# COMMAND ----------

dataframes_historicos = []

for zona_id, zona_info in zonas.items():
    df = descargar_zona(zona_id, zona_info, CONFIG_HISTORICO, "historico")
    if df is not None:
        dataframes_historicos.append(df)

print(f"Zonas procesadas: {len(dataframes_historicos)}/{len(zonas)}")

if len(dataframes_historicos) == 0:
    print("\nNo hay datos")
    dbutils.notebook.exit(json.dumps({"status": "error", "message": "No data"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONSOLIDAR NORMALIZAR

# COMMAND ----------

if len(dataframes_historicos) == 0:
    print("No hay datos para consolidar")
    dbutils.notebook.exit({"status": "error", "message": "No data downloaded"})

df_consolidado = pd.concat(dataframes_historicos, ignore_index=True)

print(f"Datos consolidados: {len(df_consolidado):,} registros")

# Normalizar columnas
columnas_requeridas = ['time', 'depth', 'latitude', 'longitude', 
                       'thetao', 'so', 'uo', 'vo', 
                       'zona_id', 'nombre_zona', 'tipo_dato']

for col in columnas_requeridas:
    if col not in df_consolidado.columns:
        df_consolidado[col] = pd.NA

df_consolidado = df_consolidado[columnas_requeridas]
df_consolidado['year_month'] = pd.to_datetime(df_consolidado['time']).dt.strftime('%Y-%m')

display(df_consolidado.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## GUARDAR EN BRONZE

# COMMAND ----------

# Convertir a Spark
df_spark = spark.createDataFrame(df_consolidado)

# Path con año-mes del período descargado
year = primer_dia_mes_anterior.year
month = f"{primer_dia_mes_anterior.month:02d}"

# Nombre descriptivo con rango de fechas
filename = f"copernicus_historico_{start_date.replace('-', '')}_{end_date.replace('-', '')}.parquet"

output_path = get_path(
    "bronze",
    f"copernicus/year={year}/month={month}/historico/{filename}"
)

print(f"Path destino:")
print(f"   {output_path}")

df_spark.coalesce(1).write.mode("overwrite").parquet(output_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LIMPIAR

# COMMAND ----------

import shutil
import json

# Limpiar temporal
if OUTPUT_DIR.exists():
    shutil.rmtree(OUTPUT_DIR)
    print(f"Archivos temporales eliminados")

result = {
    "status": "success",
    "tipo": "historico",
    "fecha_ejecucion": datetime.now().isoformat(),
    "periodo_inicio": start_date,
    "periodo_fin": end_date,
    "zonas_procesadas": len(zonas),
    "total_registros": len(df_consolidado),
    "output_path": output_path
}