import azure.functions as func
import logging
import json
import os
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient



def load_config_from_blob():
    account = os.environ["CONFIG_STORAGE_ACCOUNT"]      
    container = os.environ["CONFIG_CONTAINER"]          
    blob_path = os.environ["CONFIG_PATH"]               
    
    url = f"https://{account}.blob.core.windows.net/{container}/{blob_path}"
    cred = DefaultAzureCredential()
    blob = BlobClient.from_blob_url(url, credential=cred)

    raw = blob.download_blob().readall()
    return json.loads(raw)


def main(event: func.EventHubEvent):
    raw = event.get_body().decode("utf-8")
    logging.info(f"DISPATCH_DQ received: {raw}")

    msg = json.loads(raw)
    path = msg.get("path")
    source = msg.get("source", "").strip().lower()

    if not path:
        logging.error("Missing 'path' in event body. Skipping.")
        return

    host = os.environ["DATABRICKS_HOST"].rstrip("/")     
    token = os.environ["DATABRICKS_TOKEN"]               
    job_id = int(os.environ["DATABRICKS_JOB_ID"])        

    url = f"{host}/api/2.1/jobs/run-now"                 
    payload = {                                          
        "job_id": job_id,                               
        "notebook_params": {"path": path, "source": source}  
    }                                                    

    headers = {"Authorization": f"Bearer {token}"}      
    r = requests.post(url, headers=headers, json=payload, timeout=30)  

    if r.status_code >= 300:                            
        logging.error(f"Databricks run-now failed: {r.status_code} {r.text}")  
        raise Exception(f"Databricks run-now failed: {r.status_code}")  
    logging.info(f"Databricks run-now OK: {r.text}")    


    logging.info(f"DISPATCH DISABLED - evento recibido pero Databricks NO llamado. path={path} source={source}")
    