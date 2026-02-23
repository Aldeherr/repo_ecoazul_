###############################################################################
# outputs.tf — EcoAzul TFM
# Valores de salida importantes después de aplicar el plan.
###############################################################################

output "resource_group_name" {
  description = "Nombre del Resource Group creado"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Nombre del Storage Account (Bronze + Config)"
  value       = azurerm_storage_account.main.name
}

output "storage_account_primary_key" {
  description = "Primary Access Key del Storage Account"
  value       = azurerm_storage_account.main.primary_access_key
  sensitive   = true
}

output "key_vault_uri" {
  description = "URI del Key Vault"
  value       = azurerm_key_vault.main.vault_uri
}

output "key_vault_name" {
  description = "Nombre del Key Vault"
  value       = azurerm_key_vault.main.name
}

output "eventhub_namespace_name" {
  description = "Nombre del Event Hub Namespace"
  value       = azurerm_eventhub_namespace.main.name
}

output "eventhub_connection_string" {
  description = "Connection string del Event Hub (para configurar DispatchDQ)"
  value       = azurerm_eventhub_namespace_authorization_rule.listen_send.primary_connection_string
  sensitive   = true
}

output "databricks_workspace_url" {
  description = "URL del workspace de Databricks — usa esto para el provider.databricks.host"
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_workspace_id" {
  description = "ID numérico del workspace de Databricks"
  value       = azurerm_databricks_workspace.main.workspace_id
}

output "function_ingesta_url" {
  description = "URL de la Function App IngestaOceanografica (IngestStormGlass + IngestMarea + DispatchDQ)"
  value       = "https://${azurerm_linux_function_app.ingest_oceanografica.default_hostname}"
}

output "databricks_cluster_id" {
  description = "ID del cluster de Databricks creado"
  value       = databricks_cluster.main.id
}

output "next_steps" {
  description = "Pasos manuales necesarios después del apply"
  value       = <<-EOT
    ╔══════════════════════════════════════════════════════════════╗
    ║          PASOS POST-APPLY (hacer una vez)                    ║
    ╠══════════════════════════════════════════════════════════════╣
    ║ 1. Subir config_apis.json al container 'config' del storage  ║
    ║ 2. Desplegar las Function Apps (ver README_DEPLOY.md)        ║
    ║ 3. En Databricks: crear Unity Catalog metastore              ║
    ║    (Databricks Account > Data > Create Metastore)            ║
    ║ 4. Ejecutar en orden: 00_setup_uc_silver → 01_dim_zonas      ║
    ║    → 02_dim_puntos_muestreo → pipelines diarios              ║
    ║ 5. Conectar Power BI a Databricks (Import mode)              ║
    ║    Partner Connect > Power BI > Descargar .pbids             ║
    ╚══════════════════════════════════════════════════════════════╝
  EOT
}
