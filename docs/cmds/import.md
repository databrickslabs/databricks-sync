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
databricks-sync import --profile <profile> (--git-ssh-url | -g) <url> --artifact-dir <path>
databricks-sync import --profile <profile> (--local-git-path | -l) <path> --artifact-dir <path>
databricks-sync import --help
databricks-sync import --profile <profile> ((--git-ssh-url | -g) <url> | (--local-git-path | -l) <path>) --artifact-dir <path> [--skip-refresh] [--branch <branch name> default: master] [(--ssh-key-path | -k) <path> default: ~/.ssh/id_rsa] [(--verbosity | -v) <level>] [--version <version>] [--databricks-object-type <object-type>] [--backend-file <path>] [--destroy] [--plan] [--apply] [--revision <branch | commit | tag>]

```

### Description

**Import** retrieves stored state from a git repository then applies this to the target Databricks workspace.

### Arguments

* `--profile` - The Databricks CLI connection profile for the  source workspace. For additional information, please see the Databricks Sync [Setup instructions](https://github.com/databrickslabs/databricks-sync/blob/master/docs/setup.md). If no profile was configured for the Databricks CLI during setup, then `DEFAULT` should be passed as the value.
* `--git-ssh-url` or `-g` - The URL of the remote git repo.
* `--local-git-path` or `-l` - The path of a local git repo.
* `--artifact-dir` - Path to where the plan/state file will be saved. Optional if backend state is specified through `--backend-file`.

### Options

* `--skip-refresh` - Will not update the state of the Databricks workspace. This is used in scenarios with large workspaces having many objects.
* `--branch` - This is the git repo branch of the repo designated by `--git-ssh-url flag | --local-git-path`. If not given, the default branch is `master`.
* `--ssh-key-path` or `-k` - CLI connection profile to use. The default value is `~/.ssh/id_rsa`. This is equivalent to the `-i` switch when using `ssh`.
* `--verbosity` or `-v` - For logging, takes a value of `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
* `--version` - A version can be attached.
* `--databricks-object-type` - Identifies the Databricks-native object for which a plan will be created. By default we will plan for all objects.
* `--backend-file` - The location where backend configurations for the Terraform file will be saved.
* `--destroy` - Indicates whether you wish to destroy all the provisioned infrastructure. Default is False.
* `--plan` - Generate the Terraform plan to your infrastructure.  If set, the location will be in `<artifact-dir>/plan.out`.
* `--apply` - Apply the plan and make modifications to the infrastructure.
* `--revision` - Specify the git repo revision which can be a branch, commit, tag.

### Example

```bash
# Import the stored state of all Databricks-native objects to the DEFAULT profile from the master branch of a remote GitHub repository
databricks-sync import --profile DEFAULT export -g git@github.com:USERNAME/REPOSITORY.git

# Import the stored state of all Databricks-native objects to the with test-workspace profile from a local repository on feature-213 branch
Databricks-sync import --profile test-workspace -l /path/to/local/git/repo --branch feature-213

```

