---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-index"
description: |-
  Databricks sync databricks.
---

# Databricks-Sync

Databricks-Sync is for workspace disaster recovery. You can use it to import or export some, or all, objects within a workspace. Databricks-Sync can also be used as a migration utility.

## Dependencies

This project requires the following environment dependencies:

* [golang 1.13.x](https://golang.org/dl/)
* [terraform 0.12.x](https://www.terraform.io/downloads.html)
* [terraform-provider-databricks 0.2.x](https://registry.terraform.io/providers/databrickslabs/databricks/latest)

## Installation

0. Download: `$ git clone https://github.com/databrickslabs/terraform-provider-databricks.git`
1. Install Databricks Terraform provider: `$ curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins`
2. Ensure Terraform and Golang dependencies (install via the package manager of your choice):
    1. Verify `terraform` version 0.12.x: `$ terraform version`
    2. Verify `golang` version 1.13.x: `$ go version`
    3. Verify `make` version 3.81: `$ make --version`
3. Install other dependencies: `$ cd terraform-provider-databricks && pip install -r requirements.txt`
4. Install this project: `$ make shared install`
5. Test that the installation succeeded: `databricks-terraformer --version`

## Authentication

### [Databricks-CLI](https://docs.databricks.com/dev-tools/cli/index.html)

* [PAT Tokens](https://docs.databricks.com/dev-tools/api/latest/authentication.html)
* Username and password pair

### Authenticating with Databricks CLI credentials

When exporting or importing use a "profile" from the `~/.databrickscfg` file. It is created by the `databricks configure --token` command. Check [this page](https://docs.databricks.com/dev-tools/cli/index.html#set-up-authentication)
for more details.

## Command Reference

Databricks-Sync has two commands:

* `import` - Import objects into the databricks workspace.
* `export`- Export objects from the databricks workspace.

## Argument Reference

### Arguments for export and import

* `--git-ssh-url flag | --local-git-path` - Required and mutually exclusive - i.e. only one is allowed, but you need at least one of these two.
  * `--git-ssh-url` or `-g` - The URL of the git repo should look like `git@github.com:USERNAME/REPOSITORY.git`
  * `--local-git-path` or `-l` - The path of a local git repo `/path/to/local/git/repo`
* `--branch` (Optional) This is the git repo branch of the repo designated by `--git-ssh-url flag | --local-git-path`. If not given, the default branch is `master`.
* `--ssh-key-path` or `-k` - CLI connection profile to use. The default value is "~/.ssh/id_rsa". This is equivalent to the `-i` switch when using `ssh`.
* `--verbosity` or `-v` - For logging, takes a value of `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
* `--version` - A version can be attached.

### Export - arguments for export

* `--config-path` or `-c` - This is the relative path (to the root directory of this repo) or the full path of the yaml file which is used to drive which bjects are imported/exported.
* `--delete` - When fetching and pulling remote state this will delete any items that are managed and not retrieved.
* `--dask` - Use [dask](https://docs.dask.org/en/latest/) to parallelize the process.
* `--dry-run` - This will only log to console the actions but not commit to git remote state.
* `--tag` - Assigns metadata to the cloud resource with [AWS tags](https://docs.aws.amazon.com/general/latest/gr/aws_tagging.html) or [Azure tags](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/tag-support).

### Import - arguments for import

* `--artifact-dir` - (Required) Will be where the plan/state file be saved, required unless backend state is specified.
* `--databricks-object-type` - (Optional) This is the databricks object you wish to create a plan for. By default we will plan for all objects.
* `--backend-file` - (Optional) Please provide this as this is where your backend configuration at which your terraform file will be saved.
* `--destroy` - (Optional) This is a flag that doesn't need a value after it. This flag will indicate whether you wish to destroy all the provisioned infrastructure. Default is False.
* `--plan` - (Optional) This is a flag that doesn't need a value after it. This flag will generate the terraform plan to your infrastructure.  If set, the location will be in `<artifact-dir>/plan.out`.
* `--apply` - (Optional) This flag will apply the plan and will make modifications to your infrastructure.
* `--revision` - (Optional) This is the git repo revision which can be a branch, commit, tag.
* `--skip-refresh` - (Optional) Will be where the plan/state file be saved, required unless backend state is specified. The default is false.

## Environment variables

The following configuration attributes can be passed via environment variables:

| Argument | Environment variable |
| --: | --- |
| `--profile` | `AZURE_SOURCE_WORKSPACE` | `AZURE_TARGET_WORKSPACE`
| `--git-ssh-url` | `GIT_REPO` |
| `--branch` | `MASTER_REVISION` |
| `--revision` | `MASTER_REVISION` |
| `--destroy` | `DESTROY` |
| `--artifact-dir` | `ARTIFACT_DIR` |
| `--backup` | `BACKUP_FILE` |

### Using .env file

Change the `.env.template` to just `.env` and replace the values in `.env` with the values for your environment.  Then give this file 

## Project Support

**Important:** Projects in the `databrickslabs` GitHub account, including the Databricks Terraform Provider, are not formally supported by Databricks. They are maintained by Databricks Field teams and provided as-is. There is no service level agreement (SLA). Databricks makes no guarantees of any kind. If you discover an issue with the provider, please file a GitHub Issue on the repo, and it will be reviewed by project maintainers as time permits.