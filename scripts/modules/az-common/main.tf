variable "owner" {
  type    = string
  default = ""
}

variable "prefix" {
  type    = string
  default = ""
}

provider "azurerm" {
  version = "~> 2.14"
  features {}
}

provider "random" {
  version = "~> 2.2"
}

resource "random_string" "naming" {
  special = false
  upper   = false
  length  = 6
}

data "azurerm_client_config" "current" {
}

locals {
  // dltp - databricks labs terraform provider
  prefix = "dltp${random_string.naming.result}"
  tags = {
    Environment = "Testing"
    Owner       = var.owner
    Epoch       = random_string.naming.result
  }
}

resource "azurerm_resource_group" "example" {
  name     = "${var.prefix}-${local.prefix}-rg"
  location = "eastus2"
  tags     = local.tags
}

output "azure_region" {
  value = azurerm_resource_group.example.location
}

output "test_resource_group" {
  value = azurerm_resource_group.example.name
}

resource "azurerm_databricks_workspace" "example" {
  name                        = "${var.prefix}-${local.prefix}-workspace"
  resource_group_name         = azurerm_resource_group.example.name
  location                    = azurerm_resource_group.example.location
  sku                         = "premium"
  managed_resource_group_name = "${var.prefix}-${local.prefix}-workspace-rg"
  tags                        = local.tags
}

output "databricks_azure_workspace_resource_id" {
  // The ID of the Databricks Workspace in the Azure management plane.
  value = azurerm_databricks_workspace.example.id
}

output "databricks_host" {
  value = "https://${azurerm_databricks_workspace.example.workspace_url}/"
}

output "cloud_env" {
  value = "azure"
}

output "test_prefix" {
  value = local.prefix
}