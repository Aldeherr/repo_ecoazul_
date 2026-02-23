###############################################################################
# variables.tf — EcoAzul TFM
# Todas las variables configurables. Valores en terraform.tfvars (no en git).
###############################################################################

# ── Azure Auth ───────────────────────────────────────────────────────────────

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
}

variable "client_id" {
  description = "Service Principal Client ID (App Registration)"
  type        = string
  sensitive   = true
}

variable "client_secret" {
  description = "Service Principal Client Secret"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure Tenant ID (Directory ID)"
  type        = string
  sensitive   = true
}

# ── Proyecto ─────────────────────────────────────────────────────────────────

variable "project_name" {
  description = "Nombre corto del proyecto. Se usa en todos los resource names."
  type        = string
  default     = "ecoazul"
}

variable "environment" {
  description = "Entorno: dev | staging | prod"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region. Premium Databricks + Unity Catalog disponibles en westus3."
  type        = string
  default     = "westus3"
}

variable "tags" {
  description = "Tags comunes para todos los recursos"
  type        = map(string)
  default = {
    project     = "EcoAzul"
    environment = "dev"
    managed_by  = "terraform"
    owner       = "TFM-UCM"
  }
}

# ── Databricks ───────────────────────────────────────────────────────────────

variable "databricks_token" {
  description = "Personal Access Token de Databricks. Generar en: Settings > Developer > Access tokens."
  type        = string
  sensitive   = true
}

variable "databricks_cluster_node_type" {
  description = "Tipo de nodo para el cluster de Databricks"
  type        = string
  default     = "Standard_D4s_v3"
}

variable "databricks_spark_version" {
  description = "Runtime de Databricks. 17.3 LTS = Spark 4.0.0, Scala 2.13, compatible con Photon."
  type        = string
  default     = "17.3.x-scala2.13"
}

# ── Secrets de APIs externas ─────────────────────────────────────────────────

variable "stormglass_api_key" {
  description = "API Key de StormGlass (https://stormglass.io)"
  type        = string
  sensitive   = true
}

variable "marea_api_token" {
  description = "Token de Marea API (https://marea.ooo)"
  type        = string
  sensitive   = true
}

variable "copernicus_user" {
  description = "Usuario de Copernicus Marine (https://marine.copernicus.eu)"
  type        = string
  sensitive   = true
}

variable "copernicus_pass" {
  description = "Password de Copernicus Marine"
  type        = string
  sensitive   = true
}

# ── Storage ──────────────────────────────────────────────────────────────────

variable "storage_replication" {
  description = "Tipo de replicación del Storage Account: LRS (dev/TFM) | GRS (prod)"
  type        = string
  default     = "LRS"
}

# ── Azure Functions ──────────────────────────────────────────────────────────

variable "function_runtime_version" {
  description = "Versión del runtime de Azure Functions"
  type        = string
  default     = "~4"
}
