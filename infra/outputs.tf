output "resource_group_name" {
  description = "Name of the provisioned Azure Resource Group"
  value       = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 Storage Account"
  value       = azurerm_storage_account.adls.name
}

output "storage_dfs_endpoint" {
  description = "Primary DFS endpoint for ADLS Gen2"
  value       = azurerm_storage_account.adls.primary_dfs_endpoint
}

output "data_factory_name" {
  description = "Name of the Azure Data Factory"
  value       = azurerm_data_factory.adf.name
}

output "adf_principal_id" {
  description = "System-assigned Managed Identity Principal ID for ADF"
  value       = azurerm_data_factory.adf.identity[0].principal_id
}

output "bronze_container_name" {
  description = "Bronze layer container name"
  value       = azurerm_storage_data_lake_gen2_filesystem.bronze.name
}

output "silver_container_name" {
  description = "Silver layer container name"
  value       = azurerm_storage_data_lake_gen2_filesystem.silver.name
}

output "function_app_name" {
  description = "Name of the serverless Azure Function App"
  value       = azurerm_linux_function_app.api.name
}

output "ieso_analytics_api_url" {
  description = "Public endpoint for the DuckDB IESO Lakehouse Analytics API"
  value       = "https://${azurerm_linux_function_app.api.default_hostname}/api/ieso-analytics"
}
