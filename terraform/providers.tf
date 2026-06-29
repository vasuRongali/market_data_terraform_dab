terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }

    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

provider "azurerm" {
  features {}

  resource_provider_registrations = "none"
}

provider "databricks" {
  host = azurerm_databricks_workspace.workspace.workspace_url
}