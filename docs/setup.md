---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-setup"
description: |-
  Databricks sync databricks.
---

# Databricks-Sync

Databricks-Sync is a synchronization utility for the export and import of Databricks-native objects from one workspace to another. The primary use case for Databricks-Sync is Disaster Recovery (incremental backup and restore); however, it can be used as a one-time migration utility.

## Prerequisites

Databricks-Sync authenticates to Databricks via the Databricks CLI. The Databricks CLI should be installed and configured to authenticate to the Databricks workspace using Personal Access Tokens. For detailed instructions to install and configure the Databricks CLI, please view the official [Databricks documentation](https://docs.databricks.com/dev-tools/cli/index.html#databricks-cli). It is a best practice to set up a unique profile within the Databricks CLI for each workspace.

Check your Databricks CLI access credentials in the file `~/.databrickscfg` to verify successful authentication set up. The file should contain entries like:

```
[profile]
	host = https://<databricks-instance>
	token =  <personal-access-token>
```

Any repository management tool that supports git and SSH protocols can be used to store state instead of a local repository. Instructions for connecting with common providers through SSH are linked below.

* [GitHub](https://docs.github.com/en/github/authenticating-to-github/connecting-to-github-with-ssh)
* [Bitbucket Cloud](https://support.atlassian.com/bitbucket-cloud/docs/set-up-an-ssh-key/)
* [Bitbucket Server](https://confluence.atlassian.com/bitbucketserver/enabling-ssh-access-to-git-repositories-in-bitbucket-server-776640358.html)
* [GitLab](https://docs.gitlab.com/ee/ssh/)

Please refer to the appropriate provider documentation for steps to verify SSH authentication.

## Dependencies

When using no public internet, Databricks-Sync requires the following environment dependencies:

* [terraform 0.13.x](https://www.terraform.io/downloads.html)
* [terraform-provider-databricks 0.2.x](https://registry.terraform.io/providers/databrickslabs/databricks/latest)

The use of [tfenv](https://github.com/tfutils/tfenv) is encouraged to install and manage Terraform versions.

Terraform version can be verified using `tfenv list`. The active version will have an `*`. Use `tfenv use -version` to switch the active version to 0.13.x if necessary.

## Installation

Execute: `$ pip install git+https://github.com/databrickslabs/databricks-sync.git`

Run `databricks-sync --version' to confirm successful installation.

## Command Reference

Databricks-Sync has three commands:

* `init` - Create the export configuration file for running the export command.
* `import` - Import objects into the Databricks workspace.
* `export`- Export objects from the Databricks workspace.

## Argument Reference

### Arguments for init command

The init command contains no flags and one argument, which is the name of the file. Running this command will make 
a sample yaml configuration file used for the export command.

```bash
$ databricks-sync init <filename>
```


### Arguments for both export and import

* `--git-ssh-url flag | --local-git-path` - (Required) Required and mutually exclusive - i.e. one is allowed, but you need at least one of the following two:
  * `--git-ssh-url` or `-g` - The URL of the git repo should look like `git@github.com:USERNAME/REPOSITORY.git`
  * `--local-git-path` or `-l` - The path of a local git repo `/path/to/local/git/repo`
* `--branch` (Optional) This is the git repo branch of the repo designated by `--git-ssh-url flag | --local-git-path`. If not given, the default branch is `master`.
* `--ssh-key-path` or `-k` - (Optional) CLI connection profile to use. The default value is "~/.ssh/id_rsa". This is equivalent to the `-i` switch when using `ssh`.
* `--verbosity` or `-v` - (Optional) For logging, takes a value of `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
* `--version` - (Optional) A version can be attached.

### Export - arguments for export only

* `--config-path` or `-c` - This is the relative path (to the root directory of this repo) or the full path of the yaml file which is used to drive which objects are imported/exported.
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

## Project Support

**Important:** Projects in the `databrickslabs` GitHub account, including the Databricks Terraform Provider, are not formally supported by Databricks. They are maintained by Databricks Field teams and provided as-is. There is no service level agreement (SLA). Databricks makes no guarantees of any kind. If you discover an issue with the provider, please file a GitHub Issue on the repo, and it will be reviewed by project maintainers as time permits.
