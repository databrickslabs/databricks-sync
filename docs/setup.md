## Prerequisites

### Databricks CLI

Databricks-Sync authenticates to Databricks via the Databricks CLI. The Databricks CLI should be installed and configured to authenticate to the Databricks workspace using Personal Access Tokens. For detailed instructions to install and configure the Databricks CLI, please view the official [Databricks documentation](https://docs.databricks.com/dev-tools/cli/index.html#databricks-cli). It is a best practice to set up a unique profile within the Databricks CLI for each workspace.

Check your Databricks CLI access credentials in the file `~/.databrickscfg` to verify successful authentication set up. The file should contain entries like:

```
[profile]
	host = https://<databricks-instance>
	token =  <personal-access-token>
```

### Git Repository

Either a local repository or any repository management tool that supports SSH protocols is needed to store state. Instructions for connecting with common providers through SSH are linked below.

* [GitHub](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh)
* [Bitbucket Cloud](https://support.atlassian.com/bitbucket-cloud/docs/set-up-an-ssh-key/)
* [Bitbucket Server](https://confluence.atlassian.com/bitbucketserver/enabling-ssh-access-to-git-repositories-in-bitbucket-server-776640358.html)
* [GitLab](https://docs.gitlab.com/ee/ssh/)

## Dependencies

### Terraform

Databricks Sync requires Terraform version [0.13.x or above](https://www.terraform.io/downloads.html).

The use of [tfenv](https://github.com/tfutils/tfenv) is encouraged to install and manage Terraform versions.

1. [Install tfenv](https://github.com/tfutils/tfenv#installation)
2. Run `tfenv install <version>`

Terraform version can be verified using `tfenv list`. The active version will have an `*` if set. Use `tfenv use <version>` to set the active version to one that is 0.13.x or above if necessary.

For manual installations, refer to these instructions provided by HashiCorp Learn for [AWS](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/aws-get-started#install-terraform) and [Azure](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/azure-get-started#install-terraform).

### Databricks Terraform provider

Databricks Sync will leverage Terraform to [automatically install](https://www.hashicorp.com/blog/automatic-installation-of-third-party-providers-with-terraform-0-13) the Databricks Terraform provider when executing the `import` and `export` commands.

To install manually, find the appropriate [package](https://github.com/databrickslabs/terraform-provider-databricks/releases) and download it as a zip archive. This should be [installed in the Terraform Run Environment]().

## Databricks Sync Installation

Execute: `$ pip install git+https://github.com/databrickslabs/databricks-sync.git`

Run `databricks-sync --version` to confirm successful installation.

## Project Support

**Important:** Projects in the `databrickslabs` GitHub account, including the Databricks Terraform Provider, are not formally supported by Databricks. They are maintained by Databricks Field teams and provided as-is. There is no service level agreement (SLA). Databricks makes no guarantees of any kind. If you discover an issue with the provider, please file a GitHub Issue on the repo, and it will be reviewed by project maintainers as time permits.
