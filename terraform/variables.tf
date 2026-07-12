variable "resource_group_name" {}
variable "location" {}
variable "databricks_workspace_name" {}
variable "storage_account_name" {}
variable "access_connector_name" {}
variable "environment" {
  description = "Deployment environment name"
  default     = "dev"
}
variable "catalog_name" {
  description = "Databricks catalog name"
  default     = "dev_catalog"
}
variable "storage_credential_name" {
  description = "Databricks storage credential name"
  default     = "sc-market-data-dev"
}