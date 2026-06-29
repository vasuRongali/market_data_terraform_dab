resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_databricks_workspace" "workspace" {
  name                = var.databricks_workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
}

resource "azurerm_storage_account" "adls" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location

  account_tier             = "Standard"
  account_replication_type = "LRS"

  account_kind             = "StorageV2"
  is_hns_enabled           = true
}

resource "azurerm_databricks_access_connector" "access_connector" {
  name                = var.access_connector_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "storage_access" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.access_connector.identity[0].principal_id
}


resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_id    = azurerm_storage_account.adls.id
  container_access_type = "private"
}


resource "databricks_storage_credential" "storage_credential" {
  name = "sc-market-data-dev"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.access_connector.id
  }

  comment = "Storage credential for ADLS Gen2"
}


resource "databricks_external_location" "bronze" {
  name            = "bronze_ext_loc"
  url             = "abfss://bronze@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.storage_credential.name
  comment         = "Bronze External Location"
}

resource "databricks_external_location" "silver" {
  name            = "silver_ext_loc"
  url             = "abfss://silver@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.storage_credential.name
  comment         = "Silver External Location"
}

resource "databricks_external_location" "gold" {
  name            = "gold_ext_loc"
  url             = "abfss://gold@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.storage_credential.name
  comment         = "Gold External Location"
}


resource "databricks_catalog" "dev_catalog" {
  name    = "dev_catalog"
  comment = "Development catalog"
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.dev_catalog.name
  name         = "bronze"
  comment      = "Bronze layer schema"
}