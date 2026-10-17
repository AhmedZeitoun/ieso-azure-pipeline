# ---------------------------------------------------------------------------------------------------------------------
# Local Naming & Resource Prefixing
# ---------------------------------------------------------------------------------------------------------------------
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  # Storage accounts allow only alphanumeric lowercase, max 24 chars
  storage_account_name = substr("adlsieso${var.environment}${random_string.suffix.result}", 0, 24)
}

# ---------------------------------------------------------------------------------------------------------------------
# 1. Resource Group
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.name_prefix}"
  location = var.location
  tags     = var.tags
}

# ---------------------------------------------------------------------------------------------------------------------
# 2. Azure Data Lake Storage Gen2 (StorageV2 with Hierarchical Namespace enabled)
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_storage_account" "adls" {
  name                            = local.storage_account_name
  resource_group_name             = azurerm_resource_group.rg.name
  location                        = azurerm_resource_group.rg.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true # Critical: Enables ADLS Gen2 Hierarchical Namespace for ACID Delta Lake
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false

  tags = var.tags
}

# ---------------------------------------------------------------------------------------------------------------------
# 3. Storage Containers / Filesystems: Bronze (raw) & Silver (curated Delta Lake)
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.adls.id
}

# Directories inside filesystems for clean segregation
resource "azurerm_storage_data_lake_gen2_path" "bronze_ieso_raw" {
  path               = "ieso_raw"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.bronze.name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver_ieso_generation" {
  path               = "ieso_generation"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.silver.name
  storage_account_id = azurerm_storage_account.adls.id
  resource           = "directory"
}

# ---------------------------------------------------------------------------------------------------------------------
# 4. Azure Data Factory with System-Assigned Managed Identity
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.name_prefix}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ---------------------------------------------------------------------------------------------------------------------
# 5. Role-Based Access Control (RBAC): Grant ADF Managed Identity access to ADLS Gen2
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_role_assignment" "adf_storage_blob_data_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ---------------------------------------------------------------------------------------------------------------------
# 6. ADF Linked Service: ADLS Gen2 connection via Managed Identity (Keyless, Zero-Trust)
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "ls_adls_gen2" {
  name                 = "ls_adls_gen2"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = azurerm_storage_account.adls.primary_dfs_endpoint
  use_managed_identity = true

  depends_on = [
    azurerm_role_assignment.adf_storage_blob_data_contributor
  ]
}

# ---------------------------------------------------------------------------------------------------------------------
# 7. ADF Pipeline: Validation & Ingestion Trigger Skeleton
# ---------------------------------------------------------------------------------------------------------------------
resource "azurerm_data_factory_pipeline" "pl_ieso_bronze_to_silver" {
  name            = "pl_ieso_bronze_to_silver"
  data_factory_id = azurerm_data_factory.adf.id

  # Clean pipeline skeleton: activities will be authored in ADF Studio in Phase 4
  activities_json = jsonencode([])

  depends_on = [
    azurerm_data_factory_linked_service_data_lake_storage_gen2.ls_adls_gen2
  ]
}
