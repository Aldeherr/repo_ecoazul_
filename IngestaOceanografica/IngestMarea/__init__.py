import azure.functions as func
import requests
import json
import logging
import traceback
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
import time
import os
import pandas as pd
import io

from azure.storage.blob import BlobClient

def load_config_from_blob():
    account = os.environ["CONFIG_STORAGE_ACCOUNT"]      # dataecoazul
    container = os.environ["CONFIG_CONTAINER"]          # config
    blob_path = os.environ["CONFIG_PATH"]               # ecoazul/config/config_apis.json
    
    url = f"https://{account}.blob.core.windows.net/{container}/{blob_path}"
    cred = DefaultAzureCredential()
    blob = BlobClient.from_blob_url(url, credential=cred)

    raw = blob.download_blob().readall()
    return json.loads(raw)



def main(mytimer: func.TimerRequest) -> None:
    """
    Timer: 09:00 UTC y 17:00 UTC (04:00 AM y 12:00 PM Panama)
    Llama Marea API para 10 zonas - Pronostico 48h
    Guarda en Event Hub + Blob Storage
    """
    try:
        timestamp_ingesta = datetime.utcnow()
        hora_utc = timestamp_ingesta.hour
        
        if hora_utc == 9:
            contexto = "MADRUGADA_CRITICA"
        elif hora_utc == 17:
            contexto = "MEDIODIA_SEGUIMIENTO"
        elif hora_utc == 23:
            contexto = "NOCTURNA_PLANIFICACION"
        else:
            contexto = "MANUAL_TESTING"
        
        logging.info(f"""
        INGESTA MAREA INICIADA
        Timestamp: {timestamp_ingesta.isoformat()}
        Contexto: {contexto}
        """)
        
        # 1. Cargar configuracion
        logging.info("Cargando configuracion...")
        try:
            CONFIG = load_config_from_blob()
            logging.info("Configuracion cargada desde Blob OK")
        except Exception as e:
            logging.error(f"Error cargando config: {str(e)}")
            raise
        
        # 2. Credenciales
        logging.info("Obteniendo credenciales...")
        try:
            credential = DefaultAzureCredential()
            vault_url = "https://akv-azul-eco-data.vault.azure.net"
            secret_client = SecretClient(vault_url=vault_url, credential=credential)
            
            marea_token = secret_client.get_secret(CONFIG['apis']['marea']['api_key_vault_name']).value
            eventhub_conn = secret_client.get_secret("eventhub-connection-str").value
            storage_conn = secret_client.get_secret("storage-connection-str").value
            logging.info("Credenciales obtenidas")
        except Exception as e:
            logging.error(f"Error obteniendo credenciales: {str(e)}")
            raise
        
        # 3. Event Hub Producer
        logging.info("Creando Event Hub producer...")
        try:
            producer = EventHubProducerClient.from_connection_string(
                conn_str=eventhub_conn,
                eventhub_name="marea-raw"
            )

            producer_landed = EventHubProducerClient.from_connection_string(conn_str=eventhub_conn, 
                                                                            eventhub_name="bronze-landed")
            logging.info("Event Hub producer creado")
        except Exception as e:
            logging.error(f"Error creando Event Hub producer: {str(e)}")
            raise
        
        # 4. Blob Storage Client
        logging.info("Creando Blob Storage client...")
        try:
            blob_service_client = BlobServiceClient.from_connection_string(storage_conn)
            container_client = blob_service_client.get_container_client("bronze")
            logging.info("Blob Storage client creado")
        except Exception as e:
            logging.error(f"Error creando Blob Storage client: {str(e)}")
            raise
        
        # 5. Zonas activas
        puntos_activos = [p for p in CONFIG['puntos_muestreo'] if p.get('activo', True)]
        
        exitosos = 0
        fallidos = 0
        todos_los_eventos = []
        
        # 6. Consultar cada zona
        for idx, punto in enumerate(puntos_activos, 1):
            try:
                logging.info(f"[{idx}/{len(puntos_activos)}] {punto['id']} - {punto['zona_nombre']} ({punto['tipo_punto']})")
                
                # Llamada API Marea
                response = requests.get(
                    CONFIG['apis']['marea']['base_url'],
                    params={
                        'latitude': punto['lat'],  # CAMBIAR: zona['lat_centro'] -> punto['lat']
                        'longitude': punto['lng'], # CAMBIAR: zona['lng_centro'] -> punto['lng']
                        'duration': CONFIG['apis']['marea']['duracion_minutos'],
                        'interval': CONFIG['apis']['marea']['intervalo_minutos']
                    },
                    headers={CONFIG['apis']['marea']['header_auth']: marea_token},
                    timeout=CONFIG['apis']['marea']['timeout_segundos']
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Analizar mareas
                    alturas = [h['height'] for h in data.get('heights', [])]
                    extremos = data.get('extremes', [])
                    altas = [e for e in extremos if e.get('state') == 'HIGH TIDE']
                    bajas = [e for e in extremos if e.get('state') == 'LOW TIDE']
                    
                    # Enriquecer evento
                    evento = {
                        "punto_id": punto['id'],          
                        "zona_id": punto['zona_id'],      
                        "zona_nombre": punto['zona_nombre'],
                        "tipo_punto": punto['tipo_punto'], 
                        "lat": punto['lat'],              
                        "lng": punto['lng'],              
                        "region": punto['region'],
                        "provincias": punto['provincias'],
                        "timestamp_ingesta": timestamp_ingesta.isoformat(),
                        "contexto": contexto,
                        "source": "marea",
                        "estadisticas": {
                            "altura_min_m": round(min(alturas), 2) if alturas else None,
                            "altura_max_m": round(max(alturas), 2) if alturas else None,
                            "altura_prom_m": round(sum(alturas)/len(alturas), 2) if alturas else None,
                            "rango_marea_m": round(max(alturas) - min(alturas), 2) if alturas else None,
                            "total_pleamares": len(altas),
                            "total_bajamares": len(bajas),
                            "altura_maxima_pleamar_m": round(max([e['height'] for e in altas]), 2) if altas else None,
                            "altura_minima_bajamar_m": round(min([e['height'] for e in bajas]), 2) if bajas else None
                        },
                        "raw_data": data
                    }
                    
                    # Publicar a Event Hub
                    event_data = EventData(json.dumps(evento))
                    producer.send_batch([event_data])
                    
                    # Agregar a lista para Blob Storage
                    todos_los_eventos.append(evento)
                    
                    exitosos += 1
                    logging.info(f"   OK - Rango: {evento['estadisticas']['rango_marea_m']}m")
                    
                else:
                    logging.error(f"   HTTP {response.status_code} - {response.text[:200]}")
                    fallidos += 1
                    
            except Exception as e:
                logging.error(f"   Exception: {str(e)}")
                fallidos += 1
            
            # Pausa
            if idx < len(puntos_activos):
                time.sleep(1.0)
        
        # producer.close()
        

        # 7. Guardar en Blob Storage (Parquet)
        if len(todos_los_eventos) > 0:
            try:
                logging.info("Guardando en Blob Storage...")
                
                # Convertir a DataFrame
                df = pd.DataFrame(todos_los_eventos)
                
                # Crear estructura de carpetas
                year = timestamp_ingesta.year
                month = f"{timestamp_ingesta.month:02d}"
                day = f"{timestamp_ingesta.day:02d}"
                filename = f"marea_forecast_48h_{timestamp_ingesta.strftime('%Y%m%d_%H%M%S')}.parquet"
                
                blob_path = f"marea/year={year}/month={month}/forecast/day={day}/{filename}"
                
                # Convertir DataFrame a Parquet en memoria
                parquet_buffer = io.BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                parquet_buffer.seek(0)
                
                # Subir a Blob Storage
                blob_client = container_client.get_blob_client(blob_path)
                blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)
            
                logging.info(f"Guardado en: bronze/{blob_path}")
                
                wasbs_path= f"wasbs://bronze@dataecoazul.blob.core.windows.net/{blob_path}"
                evento_landed = {"source":"marea",
                                 "path": wasbs_path,
                                 "run_ts": timestamp_ingesta.isoformat()}
                producer_landed.send_batch([EventData(json.dumps(evento_landed))])
                logging.info(f"Evento LANDED publicado en bronze-landed: {wasbs_path}")
                
                

            except Exception as e:
                logging.error(f"Error guardando en Blob Storage: {str(e)}")
                logging.error(traceback.format_exc())
        

        try:
            producer.close()
        except Exception:
            pass

        try:
            producer_landed.close()
        except Exception:
            pass

        # 8. Resumen
        logging.info(f"""
        RESUMEN FINAL MAREA:
        Exitosos: {exitosos}/{len(puntos_activos)}
        Fallidos: {fallidos}/{len(puntos_activos)}
        Event Hub: {exitosos} eventos publicados
        Blob Storage: {len(todos_los_eventos)} registros guardados
        Duracion: {(datetime.utcnow() - timestamp_ingesta).total_seconds():.1f}s
        """)
        
    except Exception as e:
        logging.error(f"ERROR GENERAL: {str(e)}")
        logging.error(traceback.format_exc())
        raise