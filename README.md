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

# Crear Service Principal con permisos Contributor en la suscripción
az ad sp create-for-rbac \
  --name "sp-ecoazul-tfm" \
  --role Contributor \
  --scopes /subscriptions/$(az account show --query id -o tsv)
```

El comando devuelve JSON con `appId`, `password`, `tenant`. Estos son las credenciales de Terraform.

### 1.2 Configurar variables

```bash
cd terraform

# Copiar el template
cp terraform.tfvars.example terraform.tfvars

# Completar con los valores reales
# (VS Code, Notepad, o cualquier editor de texto)
```

Completar en `terraform.tfvars`:
- `subscription_id` — `az account show --query id -o tsv`
- `client_id` — `appId` del Service Principal
- `client_secret` — `password` del Service Principal
- `tenant_id` — `tenant` del Service Principal
- APIs: stormglass_api_key, marea_api_token, copernicus_user/pass
- `databricks_token` — **DEJAR VACÍO** en este primer paso (`""`)

### 1.3 Aplicar Terraform (Primera pasada — sin Databricks)

> **Importante:** El primer apply crea el workspace de Databricks pero **no puede
> configurarlo aún** (el token de Databricks todavía no existe). El provider
> `databricks` de Terraform falla si no tiene un host y token válidos, incluso
> durante un apply con `-target`. Solución: declara valores de entorno ficticios
> antes de ejecutar el primer apply para que el provider se inicialice sin error.
> Después del apply puedes eliminarlos.

```bash
cd terraform
terraform init

# -- Paso previo OBLIGATORIO: evita el error de inicialización del provider databricks --
# Linux/macOS:
export DATABRICKS_HOST=https://placeholder.azuredatabricks.net
export DATABRICKS_TOKEN=placeholder_first_apply

# Windows PowerShell:
# $env:DATABRICKS_HOST="https://placeholder.azuredatabricks.net"
# $env:DATABRICKS_TOKEN="placeholder_first_apply"

# Primera pasada: solo infraestructura Azure (ignora recursos databricks_*)
terraform apply -target=azurerm_resource_group.main \
                -target=azurerm_storage_account.main \
                -target=azurerm_storage_container.stormglass \
                -target=azurerm_storage_container.marea \
                -target=azurerm_storage_container.copernicus \
                -target=azurerm_storage_container.config \
                -target=azurerm_storage_container.functions_deploy \
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

# -- Elimina las variables de entorno temporales --
# Linux/macOS:
unset DATABRICKS_HOST && unset DATABRICKS_TOKEN
# Windows PowerShell:
# Remove-Item Env:DATABRICKS_HOST; Remove-Item Env:DATABRICKS_TOKEN
```

Después del apply, la URL del workspace de Databricks está disponible en:

```bash
terraform output databricks_workspace_url
```

### 1.4 Generar Databricks PAT

1. Abrir la URL del workspace de Databricks en el navegador
2. Navegar a: **Settings** (icono de persona arriba a la derecha) → **Developer** → **Access tokens**
3. Seleccionar **Generate new token**
4. Nombre: `terraform-token`, expiración: 90 días
5. Copiar el token generado

### 1.5 Configurar acceso Key Vault para AzureDatabricks (manual)

> **Obligatorio antes del segundo apply.** El segundo apply crea el secret scope de
> Databricks vinculado al Key Vault. Para que funcione, la aplicación `AzureDatabricks`
> debe tener permisos de lectura sobre el Key Vault. Si este paso no se hace primero,
> el apply fallará al crear el secret scope.

1. En **Azure Portal** → buscar el recurso Key Vault `akv-ecoazul-*`
2. En el menú izquierdo: **Access policies** → **Create**
3. En **Permissions**: seleccionar `Get` y `List` bajo **Secret permissions**
4. En **Principal**: buscar `AzureDatabricks` (aplicación enterprise de Azure Databricks, presente en todos los tenants)
5. Confirmar con **Review + create** → **Create**

### 1.6 Aplicar Terraform (Segunda pasada — Databricks completo)

> **Configuración recomendada del cluster**:
> - **Runtime:** Databricks 17.3 LTS — Apache Spark 4.0.0, Scala 2.13
> - **VM:** `Standard_D4s_v3` (4 vCPUs, 16 GB RAM)
> - **Modo:** Single Node
> - **Photon Acceleration:** activado
>
> Si tu suscripción no tiene cuota para `Standard_D4s_v3` en `westus3`, solicita el
> aumento en Azure Portal → Subscriptions → Usage + quotas, o cambia el valor de
> `databricks_cluster_node_type` en `terraform.tfvars` por otra familia disponible.

```bash
# Editar terraform.tfvars: agregar el databricks_token
# databricks_token = "dapi.............."

# Segundo apply: aplica todo lo que faltó
terraform apply
```

Este apply crea la Function App (con las 3 funciones del proyecto), el cluster de Databricks, sube los **17 notebooks** y crea el secret scope vinculado al Key Vault.

---

## Librerías PyPI del cluster (manual, una sola vez)

Los notebooks de Copernicus requieren tres librerías que no vienen en el runtime de Databricks y deben instalarse en el cluster desde PyPI:

| Librería | Para qué |
|---|---|
| `copernicusmarine` | Descarga de datos NetCDF desde Copernicus Marine Service |
| `netCDF4` | Lectura del formato NetCDF devuelto por Copernicus |
| `xarray` | Procesamiento de arrays multidimensionales del NetCDF |

**Cómo instalarlas:**

1. Databricks UI → **Compute** → seleccionar el cluster → **Libraries**
2. **Install new** → fuente: **PyPI**
3. Instalar en este orden: `copernicusmarine` → `netCDF4` → `xarray`
4. Esperar a que el cluster complete la instalación (estado: Installed) antes de ejecutar cualquier notebook de Copernicus

> Si los notebooks de Copernicus fallan con `ModuleNotFoundError`, la causa es siempre que alguna de estas tres librerías no está instalada en el cluster activo.

---

## FASE 2 — Unity Catalog (manual, una sola vez)

### 2.1 Verificar metastore y crear catálogo principal

Los workspaces Databricks Premium en Azure tienen Unity Catalog habilitado automáticamente
con un metastore regional (`metastore_azure_westus3`) ya asignado. **No es necesario crear
el metastore manualmente.**

**Verificar que Unity Catalog está activo** (desde la carpeta `terraform/`):

```bash
curl -s -H "Authorization: Bearer $(grep databricks_token terraform.tfvars | cut -d'"' -f2)" "$(terraform output -raw databricks_workspace_url)/api/2.1/unity-catalog/catalogs"
```

Si devuelve catálogos `system` y `samples` → Unity Catalog activo. Continúa.

**Crear el catálogo `adb_ecoazul`** desde la UI del workspace:

1. Abrir el workspace de Databricks
2. En el menú izquierdo, seleccionar **Catalog**
3. En el panel Catalog, hacer clic en el botón **"+"** (arriba a la derecha del panel)
4. Seleccionar **Create catalog**
5. **Name:** `adb_ecoazul`
6. **Storage location:** seleccionar la opción disponible en el desplegable — el nombre
   corresponde al workspace y varía según el sufijo generado por Terraform
   (ej. `adb-ecoazul-xxxxx`). Se obtiene con:
   ```bash
   terraform output databricks_workspace_url
   ```
   El sufijo al final de esa URL es el que aparecerá en el desplegable.
7. Confirmar con **Create**

> El catálogo debe llamarse exactamente `adb_ecoazul` — todos los notebooks
> del proyecto usan ese nombre para referenciar schemas y tablas.

### 2.2 Configurar el Secret Scope en Databricks

El Secret Scope se crea desde una URL especial del workspace. Los valores necesarios se obtienen desde la carpeta `terraform/`:

```bash
# DNS Name del Key Vault
az keyvault show --name $(terraform output -raw key_vault_name) --query properties.vaultUri -o tsv

# Resource ID del Key Vault
az keyvault show --name $(terraform output -raw key_vault_name) --query id -o tsv

# URL del workspace (para construir la URL del formulario)
terraform output databricks_workspace_url
```

A continuación, abrir en el navegador:

```
https://<databricks_workspace_url>/#secrets/createScope
```

Completar el formulario con los siguientes valores:

| Campo | Valor |
|---|---|
| **Scope Name** | `keyvault-scope` |
| **Manage Principal** | `All Users` |
| **DNS Name** | resultado del comando `vaultUri` (ej. `https://akv-ecoazul-xxxxx.vault.azure.net/`) |
| **Resource ID** | resultado del comando `id` (ej. `/subscriptions/.../resourceGroups/.../providers/Microsoft.KeyVault/vaults/akv-ecoazul-xxxxx`) |

Confirmar con **Create**.

### 2.3 Ejecutar notebooks de setup (una sola vez)

Ejecutar los siguientes notebooks en Databricks en este orden:

```
1. /EcoAzul/2_SILVER/00_setup_uc_silver      ← Crea catalog + schemas
2. /EcoAzul/2_SILVER/01_dim_zonas            ← Carga 10 zonas de pesca
3. /EcoAzul/1_BRONZE/AUDIT/00_UPDATE_SCHEMA_CONTRACTS ← Registra schema fingerprints
```

### 2.4 *(Opcional)* Crear SAS token para `config-blob-sas` y cargar puntos de muestreo

> **Este paso es opcional.** No es necesario para el esquema de Power BI ni para el dashboard principal. El notebook `02_dim_puntos_muestreo` queda disponible para conexiones o extensiones futuras del proyecto.

El notebook `02_dim_puntos_muestreo` lee `puntos_muestreo.csv` del contenedor `config` usando un SAS token guardado en Key Vault como `config-blob-sas`.

**Paso 1 — Subir archivos de configuración al contenedor `config`:**

1. Azure Portal → **Storage accounts** → selecciona `st-ecoazul-...`
2. Menú lateral → **Containers** → haz clic en `config`
3. Botón **Upload** → selecciona los dos archivos y súbelos:
   - `config_apis.json` (configuración de APIs para las Azure Functions)
   - `puntos_muestreo.csv` (30 puntos de muestreo para el notebook Silver)

**Paso 2 — Generar el SAS token del contenedor `config`:**

1. En el mismo Storage Account, menú lateral → **Containers** → haz clic en los `...` del contenedor `config` → **Generate SAS**
2. Configura:
   - **Permissions:** Read, List
   - **Expiry:** fecha en el futuro (ej. 1 año)
3. Haz clic en **Generate SAS token and URL**
4. Copia el valor del campo **Blob SAS token** (empieza por `?sv=...`)

**Paso 3 — Guardar el SAS token en Key Vault:**

1. Azure Portal → **Key vaults** → selecciona `akv-ecoazul-...`
2. Menú lateral → **Secrets** → **Generate/Import**
3. Configura:
   - **Name:** `config-blob-sas`
   - **Value:** pega el SAS token copiado (incluyendo el `?` inicial)
4. Haz clic en **Create**

**Paso 4 — Ejecutar el notebook desde Databricks:**

```
/EcoAzul/2_SILVER/02_dim_puntos_muestreo  ← Carga 30 puntos de muestreo
```

---

## FASE 3 — Desplegar Azure Functions

### 3.1 Desplegar la Function App

El proyecto `IngestaOceanografica/` contiene las 3 funciones (IngestStormGlass, IngestMarea, DispatchDQ) y se despliega en una sola Function App.

```bash
cd ../IngestaOceanografica

func azure functionapp publish \
  $(terraform -chdir=../terraform output -raw function_ingesta_url | sed 's|https://||' | sed 's|\..*||')
```

> **Alternativas de despliegue:**
> - **Azure Portal** → Function App → Deployment Center → GitHub / Local Git
> - **Visual Studio Code** *(método usado en este proyecto)* — instala la extensión [Azure Functions](https://marketplace.visualstudio.com/items?itemName=ms-azuretools.vscode-azurefunctions), abre la carpeta `IngestaOceanografica/`, haz clic en el icono de Azure en la barra lateral → **Functions → Deploy to Function App...** → selecciona la Function App `func-ecoazul-ingesta-...`

---

## FASE 4 — Primera Ejecución del Pipeline

Ejecuta los notebooks en este orden desde Databricks. A partir del segundo día el pipeline corre solo vía los 9 Workflows jobs (ver sección **Ejecución diaria** para crearlos).

```
BRONZE — primera ingesta:
  1. 1_BRONZE/COPERNICUS/1_forecast_daily_copernicus   ← Copernicus 72h forecast
  2. 1_BRONZE/COPERNICUS/2_historico_copernicus        ← Descarga históricos de Copernicus;
                                                          estos datos alimentan el entrenamiento del modelo ML
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

1. Abrir Power BI Desktop
2. **Obtener datos** → buscar **Azure Databricks**
3. Servidor: `<databricks_workspace_url>` (obtenido con `terraform output databricks_workspace_url`)
4. HTTP path del SQL Warehouse: Databricks → SQL Warehouses → SQL Warehouse → Connection details → HTTP path
5. Modo de conectividad: **DirectQuery**
6. Autenticación: Personal Access Token (generado en el paso 1.4)
7. Navegar a `adb_ecoazul` → schema `gold` → seleccionar las tablas

Las cuatro páginas del dashboard leen de `adb_ecoazul.gold`:
- `condiciones_actuales_zona` → semáforo SEGURO/PRECAUCIÓN/PELIGRO por zona
- `forecast_zona_hora` → pronóstico 72h (series temporales de oleaje y viento)
- `forecast_zona_hora` filtrado → ventanas óptimas de pesca
- `risk_predictions` + `feature_importance` → predicciones GBT y relevancia de variables

> El SQL Warehouse debe estar **Running** para que DirectQuery funcione. El Serverless SQL Warehouse arranca en ~5 segundos; un warehouse clásico tarda 2-3 minutos de cold start.

---

## Ejecución diaria (después del setup)

Los 9 jobs de Databricks Workflows deben crearse **manualmente** desde la UI de Databricks
una vez completadas las FASES 1, 2 y 3. No están definidos como recursos Terraform.

> Crear los jobs antes de que el pipeline necesite correr en automático (día 2 en adelante).

| # | Job | Notebook | Frecuencia | Hora Panamá (UTC) |
|---|---|---|---|---|
| 0 | `dqb_bronze_validator` | `1_BRONZE/AUDIT/01_data_quality_bronze` | Reactivo (EventHub) | — |
| 1 | `forecast_daily_copernicus` | `1_BRONZE/COPERNICUS/1_forecast_daily_copernicus` | 1×/día | 21:43 (D−1) · `0 43 3 * * ?` |
| 2 | `historico_copernicus` | `1_BRONZE/COPERNICUS/2_historico_copernicus` | 1×/mes (día 8) | 21:43 · `0 43 3 8 * ?` |
| 3 | `silver_refresh_copernicus` | `2_SILVER/06_silver_fact_copernicus` | 1×/día | 02:00 AM · `0 0 8 * * ?` |
| 4 | `silver_refresh_stormglass_marea` | `2_SILVER/04_silver_fact_stormglass` + `05_silver_fact_marea` | 3×/día | 04:15/12:15/18:15 · `0 15 9,17,23 * * ?` |
| 5 | `gold_refresh` | `3_GOLD/01_gold_forecast_zona_hora` + `02` + `03` | 3×/día | 04:45/12:45/18:45 · `0 45 9,17,23 * * ?` |
| 6 | `features_ml` | `3_GOLD/03_gold_features_ml` | 1×/día | 03:15 AM · `0 15 9 * * ?` |
| 7 | `ml_training` | `4_ML/01_train_risk_model` | 1×/semana (lunes) | 02:00 AM · `0 0 8 ? * MON` |
| 8 | `ml_scoring` | `4_ML/02_score_risk_model` | 1×/día | 03:45 AM · `0 45 9 * * ?` |

**Cómo crear cada job:**

1. Databricks UI → **Workflows** → **Create job**
2. Nombre: el indicado en la columna **Job** de la tabla
3. **Task**: tipo `Notebook`, seleccionar el notebook correspondiente del workspace `/EcoAzul/...`
4. **Cluster**: seleccionar el cluster `cluster-ecoazul-main`
5. **Schedule**: activar el schedule e introducir la expresión cron UTC de la tabla
6. Confirmar con **Create**

> Los jobs 4 (`silver_refresh_stormglass_marea`) y 5 (`gold_refresh`) tienen múltiples notebooks:
> añádelos como tareas secuenciales dentro del mismo job usando **Add task**.

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

Para escalar, modificar en `terraform.tfvars`:
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
