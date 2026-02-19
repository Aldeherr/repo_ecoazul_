# EcoAzul — Guía de Despliegue

Sistema de predicción de condiciones oceánicas para pescadores artesanales de Panamá.
Stack: Azure Functions · Databricks · Delta Lake · Unity Catalog · MLflow · Power BI

---

## Prerrequisitos

| Herramienta | Versión mínima | Para qué |
|---|---|---|
| [Terraform](https://developer.hashicorp.com/terraform/install) | ≥ 1.5 | Infraestructura Azure |
| [Azure CLI](https://learn.microsoft.com/es-es/cli/azure/install-azure-cli) | cualquiera | Autenticación + Function deploy |
| [Azure Functions Core Tools](https://learn.microsoft.com/es-es/azure/azure-functions/functions-run-local) | v4 | Desplegar el código de Functions |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) | ≥ 0.200 | Upload de notebooks (alternativa a Terraform) |
| Python | 3.12 | Functions locales |

**Cuentas externas necesarias:**
- [StormGlass](https://stormglass.io) — API Key (**plan de pago €20/mes**, 500 req/día; el proyecto usa 90)
- [Marea](https://marea.ooo) — API Token (**plan de pago $5 USD/mes**, 20.000 llamadas/mes; el proyecto usa ~2.700)
- [Copernicus Marine](https://marine.copernicus.eu) — cuenta gratuita (programa EU)

---

## Estructura del repositorio

```
TFM_PROYECT/
├── terraform/                  ← Infraestructura como código
│   ├── providers.tf            ← azurerm + databricks providers
│   ├── variables.tf            ← Todas las variables
│   ├── main.tf                 ← Recursos Azure + Databricks
│   ├── outputs.tf              ← URLs y nombres post-deploy
│   ├── terraform.tfvars.example ← Template de configuración
│   └── .gitignore              ← Excluye secretos del git
├── IngestaOceanografica/       ← Azure Functions (Python 3.12)
│   ├── IngestStormGlass/       ← Timer trigger: 04am, 12pm, 06pm (Panamá)
│   ├── IngestMarea/            ← Timer trigger: 04am, 12pm, 06pm (Panamá)
│   └── DispatchDQ/             ← EventHub trigger → Databricks DQ job
├── 1_BRONZE/                   ← Notebooks: ingesta + validación de calidad
├── 2_SILVER/                   ← Notebooks: normalización Delta Lake
├── 3_GOLD/                     ← Notebooks: aggregación analítica
├── 4_ML/                       ← Notebooks: entrenamiento + scoring GBT
└── DASHBOARD_FINAL/            ← Power BI .pbix (no en git — ver sección Power BI)
```

---

## FASE 1 — Infraestructura Azure con Terraform

### 1.1 Crear Service Principal

```bash
# Login en Azure
az login

# Crear Service Principal con permisos Contributor en tu suscripción
az ad sp create-for-rbac \
  --name "sp-ecoazul-tfm" \
  --role Contributor \
  --scopes /subscriptions/$(az account show --query id -o tsv)
```

El comando devuelve JSON con `appId`, `password`, `tenant`. Estos son tus credenciales de Terraform.

### 1.2 Configurar variables

```bash
cd terraform

# Copiar el template
cp terraform.tfvars.example terraform.tfvars

# Editar con tus valores reales
# (usa VS Code, Notepad, o cualquier editor)
```

Rellena en `terraform.tfvars`:
- `subscription_id` — `az account show --query id -o tsv`
- `client_id` — `appId` del Service Principal
- `client_secret` — `password` del Service Principal
- `tenant_id` — `tenant` del Service Principal
- APIs: stormglass_api_key, marea_api_token, copernicus_user/pass
- `databricks_token` — **DEJAR VACÍO** en este primer paso (`""`)

### 1.3 Aplicar Terraform (Primera pasada — sin Databricks)

> **Importante:** El primer apply crea el workspace de Databricks pero no puede
> configurarlo aún (necesita el token que aún no existe). Ejecuta solo los recursos
> de Azure primero.

```bash
cd terraform
terraform init

# Primera pasada: solo infraestructura Azure (ignora recursos databricks_*)
terraform apply -target=azurerm_resource_group.main \
                -target=azurerm_storage_account.main \
                -target=azurerm_storage_container.stormglass \
                -target=azurerm_storage_container.marea \
                -target=azurerm_storage_container.copernicus \
                -target=azurerm_storage_container.config \
                -target=azurerm_key_vault.main \
                -target=azurerm_key_vault_secret.stormglass_api_key \
                -target=azurerm_key_vault_secret.marea_api_token \
                -target=azurerm_key_vault_secret.copernicus_user \
                -target=azurerm_key_vault_secret.copernicus_pass \
                -target=azurerm_key_vault_secret.storage_key \
                -target=azurerm_eventhub_namespace.main \
                -target=azurerm_eventhub.bronze_landed \
                -target=azurerm_eventhub_namespace_authorization_rule.listen_send \
                -target=azurerm_key_vault_secret.eventhub_conn \
                -target=azurerm_key_vault_secret.storage_conn \
                -target=azurerm_service_plan.functions \
                -target=azurerm_databricks_workspace.main
```

Después del apply, anota la URL del workspace de Databricks:

```bash
terraform output databricks_workspace_url
```

### 1.4 Generar Databricks PAT

1. Abre la URL del workspace de Databricks en tu navegador
2. Ve a: **Settings** (icono de persona arriba a la derecha) → **Developer** → **Access tokens**
3. Haz clic en **Generate new token**
4. Nombre: `terraform-token`, expiración: 90 días
5. Copia el token generado

### 1.5 Aplicar Terraform (Segunda pasada — Databricks completo)

```bash
# Editar terraform.tfvars: agregar el databricks_token
# databricks_token = "dapi.............."

# Segundo apply: aplica todo lo que faltó
terraform apply
```

Este apply sube los 11 notebooks al workspace y crea los 9 Databricks Workflows jobs con sus calendarios y dependencias.

---

## Librerías PyPI del cluster (manual, una sola vez)

Los notebooks de Copernicus requieren tres librerías que no vienen en el runtime de Databricks y deben instalarse en el cluster desde PyPI:

| Librería | Para qué |
|---|---|
| `copernicusmarine` | Descarga de datos NetCDF desde Copernicus Marine Service |
| `netCDF4` | Lectura del formato NetCDF devuelto por Copernicus |
| `xarray` | Procesamiento de arrays multidimensionales del NetCDF |

**Cómo instalarlas:**

1. Databricks UI → **Compute** → selecciona el cluster → **Libraries**
2. **Install new** → fuente: **PyPI**
3. Instalar en este orden: `copernicusmarine` → `netCDF4` → `xarray`
4. Esperar a que el cluster las instale (estado: Installed) antes de ejecutar cualquier notebook de Copernicus

> Si los notebooks de Copernicus fallan con `ModuleNotFoundError`, la causa es siempre que alguna de estas tres librerías no está instalada en el cluster activo.

---

## FASE 2 — Unity Catalog (manual, una sola vez)

Unity Catalog requiere configuración a nivel de **Databricks Account** (no workspace).
Terraform no puede hacerlo directamente — son 3 clics en la UI.

### 2.1 Crear el Metastore

1. Ve a [accounts.azuredatabricks.net](https://accounts.azuredatabricks.net)
2. Inicia sesión con tu cuenta de administrador de Databricks
3. **Data** → **Create metastore**
   - Name: `metastore-ecoazul`
   - Region: `West US 3`
   - Storage path: `abfss://unity-catalog@<storage_account_name>.dfs.core.windows.net/`
     *(reemplaza `<storage_account_name>` con el valor de `terraform output storage_account_name`)*
4. **Assign to workspace** → selecciona el workspace `adb-ecoazul-*`

### 2.2 Configurar el Secret Scope en Databricks

```bash
# Crear el scope vinculado a Key Vault
databricks secrets create-scope \
  --scope keyvault-scope \
  --scope-backend-type AZURE_KEYVAULT \
  --resource-id $(terraform output -raw key_vault_name | xargs -I{} az keyvault show --name {} --query id -o tsv) \
  --dns-name $(terraform output -raw key_vault_uri)
```

O desde la UI de Databricks: **Settings** → **Secrets** → **Create scope**

### 2.3 Ejecutar notebooks de setup (una sola vez)

Desde Databricks, ejecuta en este orden:

```
1. /EcoAzul/2_SILVER/00_setup_uc_silver      ← Crea catalog + schemas
2. /EcoAzul/2_SILVER/01_dim_zonas            ← Carga 10 zonas de pesca
3. /EcoAzul/2_SILVER/02_dim_puntos_muestreo  ← Carga 30 puntos de muestreo
4. /EcoAzul/1_BRONZE/AUDIT/00_UPDATE_SCHEMA_CONTRACTS ← Registra schema fingerprints
```

---

## FASE 3 — Desplegar Azure Functions

### 3.1 Subir config_apis.json al Storage

```bash
az storage blob upload \
  --account-name $(terraform output -raw storage_account_name) \
  --container-name config \
  --name config_apis.json \
  --file ../IngestaOceanografica/config_apis.json
```

### 3.2 Desplegar las Function Apps

```bash
cd ../IngestaOceanografica

# IngestStormGlass
func azure functionapp publish \
  $(terraform -chdir=../terraform output -raw function_stormglass_url | sed 's|https://||' | sed 's|\..*||')

# IngestMarea
func azure functionapp publish \
  $(terraform -chdir=../terraform output -raw function_marea_url | sed 's|https://||' | sed 's|\..*||')
```

> **Alternativa más simple:** Desde Azure Portal → Function App → Deployment Center → GitHub / Local Git

---

## FASE 4 — Primera Ejecución del Pipeline

Ejecuta los notebooks en este orden desde Databricks. A partir del segundo día el pipeline corre solo vía los 9 Workflows jobs que Terraform ya creó.

```
BRONZE — primera ingesta:
  1. 1_BRONZE/COPERNICUS/1_forecast_daily_copernicus   ← Copernicus 72h forecast
  2. 1_BRONZE/COPERNICUS/2_historico_copernicus        ← Histórico del mes anterior
                                                          (OBLIGATORIO antes del ML)
  [Disparar manualmente las Azure Functions para StormGlass y Marea, o esperar
   a la próxima ronda del timer (04:00, 12:00 o 18:00 hora Panamá)]

  3. 1_BRONZE/AUDIT/01_data_quality_bronze             ← Validar archivos en Bronze

SILVER:
  4. 2_SILVER/04_silver_fact_stormglass
  5. 2_SILVER/05_silver_fact_marea
  6. 2_SILVER/06_silver_fact_copernicus

GOLD:
  7. 3_GOLD/01_gold_forecast_zona_hora        ← Hub de integración (3 fuentes)
  8. 3_GOLD/02_gold_condiciones_actuales_zona ← Semáforo SEGURO/PRECAUCIÓN/PELIGRO
  9. 3_GOLD/03_gold_features_ml               ← Vector de 29 features para ML

ML:
  10. 4_ML/01_train_risk_model    ← Entrena GBT, registra en MLflow (risk_score_predictor)
  11. 4_ML/02_score_risk_model    ← Genera gold.risk_predictions (72h de predicciones)
```

> El paso 2 (`2_historico_copernicus`) descarga el mes anterior de temperatura, salinidad y corrientes. Sin este histórico, el modelo GBT entrena con muy pocas filas y los resultados no son representativos. Ejecutarlo siempre antes del primer entrenamiento.

---

## FASE 5 — Conectar Power BI

### DirectQuery sobre SQL Warehouse

El dashboard usa **DirectQuery**: cada consulta va en tiempo real contra el SQL Warehouse de Databricks, garantizando que siempre muestra el run más reciente sin importar datos manualmente.

1. Abre Power BI Desktop
2. **Obtener datos** → busca **Azure Databricks**
3. Servidor: `<databricks_workspace_url>` (del `terraform output databricks_workspace_url`)
4. HTTP path del SQL Warehouse: Databricks → SQL Warehouses → tu warehouse → Connection details → HTTP path
5. Modo de conectividad: **DirectQuery**
6. Autenticación: Personal Access Token (el mismo del paso 1.4)
7. Navega a `adb_ecoazul` → schema `gold` → selecciona las tablas

Las cuatro páginas del dashboard leen de `adb_ecoazul.gold`:
- `condiciones_actuales_zona` → semáforo SEGURO/PRECAUCIÓN/PELIGRO por zona
- `forecast_zona_hora` → pronóstico 72h (series temporales de oleaje y viento)
- `forecast_zona_hora` filtrado → ventanas óptimas de pesca
- `risk_predictions` + `feature_importance` → predicciones GBT y relevancia de variables

> El SQL Warehouse debe estar **Running** para que DirectQuery funcione. El Serverless SQL Warehouse arranca en ~5 segundos; un warehouse clásico tarda 2-3 minutos de cold start.

---

## Ejecución diaria (después del setup)

Los 9 jobs de Databricks Workflows están definidos en `terraform/main.tf` y se crean automáticamente con el segundo `terraform apply`. No hay que configurarlos manualmente.

| # | Job | Frecuencia | Hora Panamá |
|---|---|---|---|
| 0 | `dqb_bronze_validator` | Reactivo (EventHub) | — |
| 1 | `forecast_daily_copernicus` | 1×/día | 21:43 (D−1) |
| 2 | `historico_copernicus` | 1×/mes (día 8) | 21:43 |
| 3 | `silver_refresh_copernicus` | 1×/día | 02:00 AM |
| 4 | `silver_refresh_stormglass_marea` | 3×/día | 04:15 / 12:15 / 18:15 |
| 5 | `gold_refresh` | 3×/día | 04:45 / 12:45 / 18:45 |
| 6 | `features_ml` | 1×/día | 03:15 AM |
| 7 | `ml_training` | 1×/semana (lunes) | 02:00 AM |
| 8 | `ml_scoring` | 1×/día | 03:45 AM |

Las Azure Functions disparan a las 04:00, 12:00 y 18:00 hora Panamá (`cron: 0 0 9,17,23 * * *` UTC).

---

## Escalabilidad — Pasar de TFM a Producción

| Aspecto | TFM (actual) | Producción |
|---|---|---|
| Cluster | Standard_DS4_v2, modo general | Auto-scaling 2-8 nodos |
| Storage | LRS (1 copia) | GRS (geo-redundante) |
| Event Hub | Basic (1 día retención) | Standard (7 días) |
| Functions | Consumption (cold start) | Premium (always warm) |
| Key Vault | Soft-delete 7 días | Purge protection ON |
| Databricks | 1 workspace | Multi-workspace + VNET |
| Monitoring | Manual (logs Azure) | Azure Monitor + alertas |

Para escalar, solo cambia en `terraform.tfvars`:
```hcl
storage_replication          = "GRS"     # LRS → GRS
databricks_cluster_node_type = "Standard_DS4_v2"
# + cambiar sku del Event Hub a "Standard" en main.tf
```

---

## Costes reales (westus3, región West US 3 — febrero 2026)

Datos obtenidos del portal Azure tras ~18 días de operación:

| Recurso | Observado parcial | Estimación mes completo |
|---|---|---|
| Azure Databricks Service (DBU) | $46.02 | ~$77 |
| Event Hubs namespace | $28.20 | ~$47 |
| Storage Account (WASBS) | $0.57 | ~$1 |
| Azure Functions (Consumption) | $0.00 | $0 |
| Key Vault | <$0.01 | ~$0 |
| NAT Gateway + Public IP (managed RG) | $20.21 | ~$34 |
| VMs del cluster (managed RG) | ~$21 | ~$35 |
| Storage interno Databricks | $0.59 | ~$1 |
| **Total Azure** | **$121.55** | **~$203** |
| StormGlass API | — | €20 (~$22) |
| Marea.info API | — | $5 |
| **Total general** | | **~$230/mes** |

> El componente más caro es Databricks (~$155/mes entre DBU e infraestructura subyacente). El Event Hub (~$47/mes) resulta más caro de lo esperado por la configuración del namespace en producción.

---

*EcoAzul TFM — Universidad Complutense de Madrid — 2025*
