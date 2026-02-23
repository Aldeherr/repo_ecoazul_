###############################################################################
# main.tf — EcoAzul TFM
# Infraestructura completa: Storage, Key Vault, Event Hub, Functions, Databricks
###############################################################################

# Suffix aleatorio para evitar conflictos de nombres globales (storage, keyvault)
resource "random_string" "suffix" {
  length  = 5
  special = false
  upper   = false
}

locals {
  suffix   = random_string.suffix.result
  rg_name  = "rg-${var.project_name}-${var.environment}"
  sa_name  = "st${var.project_name}${local.suffix}"    # max 24 chars, lowercase
  kv_name  = "akv-${var.project_name}-${local.suffix}" # max 24 chars
}

# =============================================================================
# RESOURCE GROUP
# =============================================================================

resource "azurerm_resource_group" "main" {
  name     = local.rg_name
  location = var.location
  tags     = var.tags
}

# =============================================================================
# STORAGE ACCOUNT — Bronze Parquets + Config + Functions
# =============================================================================

resource "azurerm_storage_account" "main" {
  name                     = local.sa_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Hierarchical Namespace = ADLS Gen2 (necesario para Databricks)
  tags                     = var.tags
}

# Contenedores Bronze (datos crudos en Parquet con particionado Hive)
resource "azurerm_storage_container" "stormglass" {
  name                  = "stormglass"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "marea" {
  name                  = "marea"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "copernicus" {
  name                  = "copernicus"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# Contenedor de configuración (config_apis.json, puntos_muestreo, etc.)
resource "azurerm_storage_container" "config" {
  name                  = "config"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# Contenedor para Azure Functions deployment package
resource "azurerm_storage_container" "functions_deploy" {
  name                  = "azure-webjobs-hosts"
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

# =============================================================================
# AZURE KEY VAULT — Secretos (akv-azul-eco-data equivalente)
# =============================================================================

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                       = local.kv_name
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false  # false para TFM — en prod cambiar a true
  tags                       = var.tags

  # Acceso para el Service Principal que ejecuta Terraform
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Purge", "Recover"
    ]
  }
}

# Secrets de APIs externas
resource "azurerm_key_vault_secret" "stormglass_api_key" {
  name         = "stormglass-api-key"
  value        = var.stormglass_api_key
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "marea_api_token" {
  name         = "marea-api-token"
  value        = var.marea_api_token
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "copernicus_user" {
  name         = "copernicus-user"
  value        = var.copernicus_user
  key_vault_id = azurerm_key_vault.main.id
}

resource "azurerm_key_vault_secret" "copernicus_pass" {
  name         = "copernicus-pass"
  value        = var.copernicus_pass
  key_vault_id = azurerm_key_vault.main.id
}

# Storage key para que Databricks acceda al ADLS Gen2
resource "azurerm_key_vault_secret" "storage_key" {
  name         = "storage-key"
  value        = azurerm_storage_account.main.primary_access_key
  key_vault_id = azurerm_key_vault.main.id
}

# Connection string de Event Hub (se actualiza después de crear el EH)
resource "azurerm_key_vault_secret" "eventhub_conn" {
  name         = "eventhub-connection-str"
  value        = azurerm_eventhub_namespace_authorization_rule.listen_send.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

# Connection string del Storage para las Functions
resource "azurerm_key_vault_secret" "storage_conn" {
  name         = "storage-connection-str"
  value        = azurerm_storage_account.main.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
}

# =============================================================================
# EVENT HUB — Notificaciones Bronze Landing (Basic tier)
# =============================================================================

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${var.project_name}-${local.suffix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Basic"      # Basic: suficiente para 1 consumer group
  capacity            = 1
  tags                = var.tags
}

resource "azurerm_eventhub" "bronze_landed" {
  name                = "bronze-landed"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 2    # Basic mínimo = 2
  message_retention   = 1    # Basic máximo = 1 día
}

# Authorization Rule para Publish (Functions Bronze → Event Hub)
resource "azurerm_eventhub_namespace_authorization_rule" "listen_send" {
  name                = "listen-send-policy"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = true
  manage              = false
}

# =============================================================================
# APP SERVICE PLAN — Consumption (serverless, gratis para volumen de TFM)
# =============================================================================

resource "azurerm_service_plan" "functions" {
  name                = "asp-${var.project_name}-functions"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "Y1"   # Consumption plan = serverless
  tags                = var.tags
}

# =============================================================================
# AZURE FUNCTION APP — IngestaOceanografica
# Contiene las 3 funciones del proyecto: IngestStormGlass, IngestMarea, DispatchDQ
# =============================================================================

resource "azurerm_linux_function_app" "ingest_oceanografica" {
  name                       = "func-${var.project_name}-ingesta-${local.suffix}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  service_plan_id            = azurerm_service_plan.functions.id
  storage_account_name       = azurerm_storage_account.main.name
  storage_account_access_key = azurerm_storage_account.main.primary_access_key
  tags                       = var.tags

  # Identidad gestionada — para acceder a Key Vault sin credenciales hardcodeadas
  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.12"
    }
  }

  app_settings = {
    # Configuración básica del runtime
    FUNCTIONS_WORKER_RUNTIME    = "python"
    FUNCTIONS_EXTENSION_VERSION = var.function_runtime_version
    AzureWebJobsStorage         = azurerm_storage_account.main.primary_connection_string
    WEBSITE_RUN_FROM_PACKAGE    = "1"

    # API Keys (IngestStormGlass e IngestMarea via Key Vault)
    STORMGLASS_API_KEY = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.stormglass_api_key.id})"
    MAREA_API_TOKEN    = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.marea_api_token.id})"

    # Event Hub (compartido por las 3 funciones)
    EVENTHUB_CONN_STR = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.eventhub_conn.id})"
    EVENTHUB_NAME     = azurerm_eventhub.bronze_landed.name

    # Storage (IngestStormGlass e IngestMarea)
    STORAGE_CONN_STR     = "@Microsoft.KeyVault(SecretUri=${azurerm_key_vault_secret.storage_conn.id})"
    STORAGE_ACCOUNT_NAME = azurerm_storage_account.main.name
    CONFIG_CONTAINER     = "config"
    CONFIG_BLOB_NAME     = "config_apis.json"

    # Databricks (DispatchDQ — dispara el job de calidad de datos)
    DATABRICKS_HOST = "https://${azurerm_databricks_workspace.main.workspace_url}"
    DATABRICKS_TOKEN = var.databricks_token
    # DQ_JOB_ID se actualiza manualmente tras crear el Job en Databricks UI
    DQ_JOB_ID = "0"
  }
}

# Permiso Key Vault → Function App IngestaOceanografica (managed identity)
resource "azurerm_key_vault_access_policy" "func_ingesta" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = azurerm_linux_function_app.ingest_oceanografica.identity[0].tenant_id
  object_id    = azurerm_linux_function_app.ingest_oceanografica.identity[0].principal_id

  secret_permissions = ["Get", "List"]
}

# =============================================================================
# DATABRICKS WORKSPACE — Premium (requerido para Unity Catalog)
# =============================================================================

resource "azurerm_databricks_workspace" "main" {
  name                        = "adb-${var.project_name}-${local.suffix}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"   # Premium = Unity Catalog + MLflow Model Registry
  managed_resource_group_name = "rg-${var.project_name}-databricks-managed"
  tags                        = var.tags

  # custom_parameters omitido — Databricks gestiona su propio storage account (DBFS) con nombre único
}

# =============================================================================
# DATABRICKS — Cluster de cómputo (via provider databricks)
# NOTA: Este bloque requiere la fase 2 del despliegue (ver README_DEPLOY.md)
# =============================================================================

resource "databricks_cluster" "main" {
  cluster_name            = "cluster-ecoazul-main"
  spark_version           = var.databricks_spark_version
  node_type_id            = var.databricks_cluster_node_type
  autotermination_minutes = 30

  # Single node para TFM (reduce coste). En prod usar autoscale.
  # SINGLE_USER requerido por Unity Catalog — NO_ISOLATION no está permitido en este workspace.
  data_security_mode = "SINGLE_USER"
  runtime_engine     = "PHOTON"
  num_workers        = 0
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"

    # Configuración ADLS Gen2 con clave del Storage Account
    "fs.azure.account.key.${azurerm_storage_account.main.name}.dfs.core.windows.net" = azurerm_storage_account.main.primary_access_key
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
    "project"       = "EcoAzul"
  }

  library {
    pypi { package = "copernicusmarine" }
  }
  library {
    pypi { package = "pandas" }
  }
  library {
    pypi { package = "pyarrow" }
  }
  library {
    pypi { package = "azure-eventhub" }
  }
  library {
    pypi { package = "azure-storage-blob" }
  }
}

# =============================================================================
# DATABRICKS — Secret Scope vinculado a Key Vault
# =============================================================================

# NOTA: El secret scope de Key Vault se crea manualmente (ver README sección 2.2).
# Requiere autenticación AAD que el provider PAT no soporta.
# NOTA: El access policy de Databricks → Key Vault se configura manualmente en Azure Portal.
# Key Vault → Access policies → Add → buscar "AzureDatabricks" → permisos Get y List en Secrets.

# =============================================================================
# DATABRICKS — Notebooks upload (19 notebooks del proyecto)
# =============================================================================

# -- BRONZE: AUDIT --
resource "databricks_notebook" "dq_bronze" {
  path     = "/EcoAzul/1_BRONZE/AUDIT/01_data_quality_bronze"
  language = "PYTHON"
  source   = "${path.module}/../1_BRONZE/AUDIT/01_data_quality_bronze.py"
}

resource "databricks_notebook" "dq_bronze_batch" {
  path     = "/EcoAzul/1_BRONZE/AUDIT/01_data_quality_bronze_batch"
  language = "PYTHON"
  source   = "${path.module}/../1_BRONZE/AUDIT/01_data_quality_bronze_batch.py"
}

resource "databricks_notebook" "bronze_file_landed" {
  path     = "/EcoAzul/1_BRONZE/AUDIT/03_bronze_file_landed"
  language = "PYTHON"
  source   = "${path.module}/../1_BRONZE/AUDIT/03_bronze_file_landed.py"
}

resource "databricks_notebook" "update_schema_contracts" {
  path     = "/EcoAzul/1_BRONZE/AUDIT/00_UPDATE_SCHEMA_CONTRACTS"
  language = "PYTHON"
  source   = "${path.module}/../1_BRONZE/AUDIT/00_UPDATE_SCHEMA_CONTRACTS.py"
}

# -- BRONZE: COPERNICUS --
resource "databricks_notebook" "copernicus_forecast" {
  path     = "/EcoAzul/1_BRONZE/COPERNICUS/1_FORECAST_DAILY_COPERNICUS"
  language = "PYTHON"
  source   = "${path.module}/../1_BRONZE/COPERNICUS/1_forecast_daily_copernicus.py"
}

resource "databricks_notebook" "copernicus_historico" {
  path     = "/EcoAzul/1_BRONZE/COPERNICUS/2_HISTORICO_COPERNICUS"
  language = "PYTHON"
  source   = "${path.module}/../1_BRONZE/COPERNICUS/2_HISTORICO_COPERNICUS.py"
}


# -- SILVER --
resource "databricks_notebook" "setup_uc_silver" {
  path     = "/EcoAzul/2_SILVER/00_setup_uc_silver"
  language = "PYTHON"
  source   = "${path.module}/../2_SILVER/00_setup_uc_silver.py"
}

resource "databricks_notebook" "dim_zonas" {
  path     = "/EcoAzul/2_SILVER/01_dim_zonas"
  language = "PYTHON"
  source   = "${path.module}/../2_SILVER/01_dim_zonas.py"
}

resource "databricks_notebook" "dim_puntos" {
  path     = "/EcoAzul/2_SILVER/02_dim_puntos_muestreo"
  language = "PYTHON"
  source   = "${path.module}/../2_SILVER/02_dim_puntos_muestreo.py"
}

resource "databricks_notebook" "silver_stormglass" {
  path     = "/EcoAzul/2_SILVER/04_silver_fact_stormglass"
  language = "PYTHON"
  source   = "${path.module}/../2_SILVER/04_silver_fact_stormglass.py"
}

resource "databricks_notebook" "silver_marea" {
  path     = "/EcoAzul/2_SILVER/05_silver_fact_marea"
  language = "PYTHON"
  source   = "${path.module}/../2_SILVER/05_silver_fact_marea.py"
}

resource "databricks_notebook" "silver_copernicus" {
  path     = "/EcoAzul/2_SILVER/06_silver_fact_copernicus"
  language = "PYTHON"
  source   = "${path.module}/../2_SILVER/06_silver_fact_copernicus.py"
}

# -- GOLD --
resource "databricks_notebook" "gold_forecast_zona_hora" {
  path     = "/EcoAzul/3_GOLD/01_gold_forecast_zona_hora"
  language = "PYTHON"
  source   = "${path.module}/../3_GOLD/01_gold_forecast_zona_hora.py"
}

resource "databricks_notebook" "gold_condiciones_actuales" {
  path     = "/EcoAzul/3_GOLD/02_gold_condiciones_actuales_zona"
  language = "PYTHON"
  source   = "${path.module}/../3_GOLD/02_gold_condiciones_actuales_zona.py"
}

resource "databricks_notebook" "gold_features_ml" {
  path     = "/EcoAzul/3_GOLD/03_gold_features_ml"
  language = "PYTHON"
  source   = "${path.module}/../3_GOLD/03_gold_features_ml.py"
}


# -- ML --
resource "databricks_notebook" "train_risk_model" {
  path     = "/EcoAzul/4_ML/01_train_risk_model"
  language = "PYTHON"
  source   = "${path.module}/../4_ML/01_train_risk_model.py"
}

resource "databricks_notebook" "score_risk_model" {
  path     = "/EcoAzul/4_ML/02_score_risk_model"
  language = "PYTHON"
  source   = "${path.module}/../4_ML/02_score_risk_model.py"
}
