# Prerequisites

Set up the prerequisites before you begin the installation on the [setup page](setup.md).

## Authentication

### [Databricks-CLI](https://docs.databricks.com/dev-tools/cli/index.html)

* [PAT Tokens](https://docs.databricks.com/dev-tools/api/latest/authentication.html)
* Username and password pair

### Authenticating with Databricks CLI credentials

When exporting or importing use a "profile" from the `~/.databrickscfg` file. It is created by the `databricks configure --token` command. Check [this page](https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication)
for more details.

## Offline

If you will be using no public connectivity, then you may need to download the Databricks terraform provider before installation.

1. Download: `$ git clone https://github.com/databrickslabs/terraform-provider-databricks.git`
2. Install Databricks Terraform provider: `$ curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins`
