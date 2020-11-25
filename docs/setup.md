---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-setup"
description: |-
  Databricks sync databricks.
---

# Databricks-Sync

Databricks-Sync is for workspace disaster recovery. You can use it to import or export some, or all, objects within a workspace. Databricks-Sync can also be used as a migration utility.

## Prerequisites

Visit the [prerequisites page](prerequisites.md) to check if you are missing any prerequisites before beginning installation.

## Dependencies

This project requires the following environment dependencies:

* [terraform 0.13.x](https://www.terraform.io/downloads.html)
* [terraform-provider-databricks 0.2.x](https://registry.terraform.io/providers/databrickslabs/databricks/latest)

## Installation

1. Ensure Terraform dependencies (install via the package manager of your choice):
    1. Verify `terraform` version 0.13.x: `$ terraform version`
    2. Verify `make` version 3.81: `$ make --version`
2. Install other dependencies: `$ cd terraform-provider-databricks && pip install -r requirements.txt`
3. Install this project: `$ make shared install`
4. Test that the installation succeeded: `databricks-terraformer --version`

## Command Reference

Databricks-Sync has two commands:

* `import` - Import objects into the databricks workspace.
* `export`- Export objects from the databricks workspace.

## Argument Reference

### Arguments for both export and import

* `--git-ssh-url flag | --local-git-path` - (Required) Required and mutually exclusive - i.e. only one is allowed, but you need at least one of the following two:
  * `--git-ssh-url` or `-g` - The URL of the git repo should look like `git@github.com:USERNAME/REPOSITORY.git`
  * `--local-git-path` or `-l` - The path of a local git repo `/path/to/local/git/repo`
* `--branch` (Optional) This is the git repo branch of the repo designated by `--git-ssh-url flag | --local-git-path`. If not given, the default branch is `master`.
* `--ssh-key-path` or `-k` - (Optional) CLI connection profile to use. The default value is "~/.ssh/id_rsa". This is equivalent to the `-i` switch when using `ssh`.
* `--verbosity` or `-v` - (Optional) For logging, takes a value of `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
* `--version` - (Optional) A version can be attached.

### Export - arguments for export only

* `--config-path` or `-c` - This is the relative path (to the root directory of this repo) or the full path of the yaml file which is used to drive which bjects are imported/exported.
* `--dask` - (Optional) This is a flag to use [dask](https://docs.dask.org/en/latest/) to parallelize the process.
* `--dry-run` - This flag will only log to console the actions but not commit to git remote state.
* `--tag` - (Optional) Assigns metadata to the cloud resource with [AWS tags](https://docs.aws.amazon.com/general/latest/gr/aws_tagging.html) or [Azure tags](https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/tag-support).

### Import - arguments for import only

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
| `--profile` | `AZURE_SOURCE_WORKSPACE` or `AZURE_TARGET_WORKSPACE` |
| `--git-ssh-url` | `GIT_REPO` |
| `--branch` | `MASTER_REVISION` |
| `--revision` | `MASTER_REVISION` |
| `--destroy` | `DESTROY` |
| `--artifact-dir` | `ARTIFACT_DIR` |
| `--backup` | `BACKUP_FILE` |

## Project Support

**Important:** Projects in the `databrickslabs` GitHub account, including the Databricks Terraform Provider, are not formally supported by Databricks. They are maintained by Databricks Field teams and provided as-is. There is no service level agreement (SLA). Databricks makes no guarantees of any kind. If you discover an issue with the provider, please file a GitHub Issue on the repo, and it will be reviewed by project maintainers as time permits.
