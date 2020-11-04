provider "azurerm" {
  version = "~> 2.14"
  features {}
}

terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.2.7"
    }
  }
}


data "azurerm_client_config" "current" {
}

data "external" "me" {
  program = ["az", "account", "show", "--query", "user"]
}

module "src_az_ws" {
  source = "../modules/az-common"
  owner  = lookup(data.external.me.result, "name")
}

module "tgt_az_ws" {
  source = "../modules/az-common"
  owner  = lookup(data.external.me.result, "name")
}

output "cloud_env" {
  value = "azure"
}

output "src_resource_group" {
  value = module.src_az_ws.test_resource_group
}

output "src_region" {
  value = module.src_az_ws.azure_region
}

output "src_workspace_resource_id" {
  value = module.src_az_ws.databricks_azure_workspace_resource_id
}

output "src_workspace_url" {
  value = module.tgt_az_ws.databricks_host
}

output "tgt_resource_group" {
  value = module.tgt_az_ws.test_resource_group
}

output "tgt_region" {
  value = module.tgt_az_ws.azure_region
}

output "tgt_workspace_resource_id" {
  value = module.tgt_az_ws.databricks_azure_workspace_resource_id
}

output "tgt_workspace_url" {
  value = module.tgt_az_ws.databricks_host
}

provider "databricks" {
  alias = "src_workspace"
  azure_workspace_resource_id = module.src_az_ws.databricks_azure_workspace_resource_id
}

// create PAT token to provision entities within workspace
resource "databricks_token" "src_pat" {
  provider = databricks.src_workspace
  comment  = "Terraform Provisioning"
  // 100 day token
  lifetime_seconds = 8640000
}

output "src_token" {
  value = databricks_token.src_pat.token_value
  sensitive = true
}

provider "databricks" {
  alias = "tgt_workspace"
  azure_workspace_resource_id = module.tgt_az_ws.databricks_azure_workspace_resource_id
}

// create PAT token to provision entities within workspace
resource "databricks_token" "tgt_pat" {
  provider = databricks.tgt_workspace
  comment  = "Terraform Provisioning"
  // 100 day token
  lifetime_seconds = 8640000
}

output "tgt_token" {
  value = databricks_token.tgt_pat.token_value
  sensitive = true
}