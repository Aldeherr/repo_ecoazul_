###############################################################################
# providers.tf — EcoAzul TFM
# Proveedores: azurerm (infraestructura Azure) + databricks (workspace config)
###############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.36"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# --- Azure (usa las variables de autenticación del Service Principal) ---
provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
  }

  subscription_id = var.subscription_id
  client_id       = var.client_id
  client_secret   = var.client_secret
  tenant_id       = var.tenant_id
}

# --- Databricks (se configura DESPUÉS de que el workspace existe) ---
# NOTA: Este provider requiere que el workspace ya esté creado.
# En el primer `terraform apply` puede fallar si el workspace no existe aún.
# Solución: ver README_DEPLOY.md sección "Despliegue en dos fases".
provider "databricks" {
  host  = azurerm_databricks_workspace.main.workspace_url
  token = var.databricks_token   # PAT generado manualmente — ver README
}
