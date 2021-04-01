---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-command-ref-import"
description: |-
  Databricks sync databricks commands import.
---

## Import

### Synopsis

```bash
TF_VAR_PASSIVE="text" TF_VAR_CLOUD="text" GIT_PYTHON_TRACE="text" databricks-sync import [--profile DATABRICKS_PROFILE_NAME default="DEFAULT"] {-l, --local-git-path PATH | -g --git-ssh-url REPO_URL} [--branch BRANCH_NAME] [--revision {BRANCH | COMMIT | TAG}] [-k, --ssh-key-path] --artifact-dir PATH [--backend-file PATH] [--databricks-object-type {CLUSTER_POLICY | DBFS_FILE | NOTEBOOK | IDENTITY | INSTANCE_POOL | INSTANCE_PROFILE | SECRETS | CLUSTER | JOB}] [--plan] [--skip-refresh] [--apply] [--destroy] [--debug]
databricks-sync import -h, --help
```

### Description

`import` retrieves stored state from a git repository then applies this to the target Databricks workspace.

### Environment Variables

* `TF_VAR_PASSIVE` - Determines if databricks-sync will run in Passive Mode. The default value is set to `False` for DR scenarios; however, it can be set `True` for migrations.
* `TF_VAR_CLOUD` - Takes a value of `was` or `azure` to specify the Cloud provider.
* `GIT_PYTHON_TRACE` - Prints all the git commands run by databricks-sync. Valid value is 'full'.

### Options

* `--profile` - The Databricks CLI connection profile for the  source workspace. For additional information, please see the Databricks Sync [Setup instructions](https://github.com/databrickslabs/databricks-sync/blob/master/docs/setup.md). If no profile was configured for the Databricks CLI during setup, then `DEFAULT` should be passed as the value.
* `-l, --local-git-path PATH` - The path of a local git repo to manage export and import. Cannot be supplied in conjunction with `-g, --git-ssh-url`.
* `-g, --git-ssh-url REPO_URL` - The URL of the remote git repo to manage export and import. Cannot be supplied in conjunction with `-l | --local-git-path`.
* `--branch` - This is the branch of the git repo designated by `{-g, --git-ssh-url flag | -l, --local-git-path}`. If not given, the default branch is `main`.
* `--revision {BRANCH | COMMIT | TAG}` - Specify the git repo revision which can be a branch, commit, tag.
* `-k, --ssh-key-path` - CLI connection profile to use. The default value is `~/.ssh/id_rsa`. This is equivalent to the `-i` switch when using `ssh`.
* `--artifact-dir PATH` - Path to where the plan/state file will be saved. Optional if backend state is specified through `--backend-file`.
* `--backend-file PATH` - The location where backend configurations for the Terraform file will be saved.
* `--databricks-object-type {CLUSTER_POLICY | DBFS_FILE | NOTEBOOK | IDENTITY | INSTANCE_POOL | INSTANCE_PROFILE | SECRETS | CLUSTER | JOB}` - This is the Databricks-native object for which to create a plan. By default, databricks-sync will plan for all objects.
* `--plan` - Generates a Terraform plan for the infrastructure
* `--skip-refresh` - Determines whether the remote state will be refreshed or not
* `--apply` - Apply the plan and make modifications to the infrastructure
* `--destroy` - Indicates whether you wish to destroy all the provisioned infrastructure
* `--debug` - Debug Mode. Shows full stack trace on error.
* `-h, --help` - Shows Usage, Options, and Arguments then exits.


### Example

```bash
# Pre-apply Plan
TF_VAR_CLOUD=azure GIT_PYTHON_TRACE=full databricks-sync -v debug import --profile DATABRICKS_PROFILE_NAME -l ~/DBFS_LOCAL_REPO_NAME --artifact-dir /dbfs/PATH --backend-file dbfs/PATH/FILES-BACKEND-CONFIG.json --plan --skip-refresh

# Apply Plan
TF_VAR_CLOUD=azure GIT_PYTHON_TRACE=full databricks-sync -v debug import --profile DATABRICKS_PROFILE_NAME -l ~/DBFS_LOCAL_REPO_NAME --artifact-dir /dbfs/PATH --backend-file dbfs/PATH/FILES-BACKEND-CONFIG.json --plan --skip-refresh --apply

# Post-apply Plan
TF_VAR_CLOUD=azure GIT_PYTHON_TRACE=full databricks-sync -v debug import --profile DATABRICKS_PROFILE_NAME -l ~/DBFS_LOCAL_REPO_NAME --artifact-dir /dbfs/PATH --backend-file dbfs/PATH/FILES-BACKEND-CONFIG.json --plan --skip-refresh
```
