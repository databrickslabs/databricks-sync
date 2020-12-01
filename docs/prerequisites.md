# Prerequisites

Set up the prerequisites before you begin the installation on the [setup page](setup.md).

## Authentication

### Authenticating to Databricks

Databricks-sync uses the Databricks CLI.

#### [Databricks-CLI](https://docs.databricks.com/dev-tools/cli/index.html)

* [PAT Tokens](https://docs.databricks.com/dev-tools/api/latest/authentication.html)
* Username and password pair

#### Authenticating with Databricks CLI credentials

When exporting or importing use a "profile" from the `~/.databrickscfg` file. It is created by the `databricks configure --token` command. Check [this page](https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication)
for more details.

### Authenticating to GitHub

If using GitHub to store state (instead of local git repo), you may need to set up [ssh authentication with Github](https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/connecting-to-github-with-ssh).

## Offline

If you will be using no public connectivity, then you may need to download the Databricks terraform provider, and tfenv, before installation.

1. Download: `$ brew install tfenv` - [tfenv documentation](https://github.com/tfutils/tfenv)
2. Download: `$ git clone https://github.com/databrickslabs/terraform-provider-databricks.git`
3. Install Databricks Terraform provider: `$ curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins`
4. Test that the installation succeeded: `databricks-terraformer --version`