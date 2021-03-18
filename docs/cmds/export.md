---
layout: "databricks"
page_title: "Provider: Databricks"
sidebar_current: "docs-databricks-command-ref-export"
description: |-
  Databricks sync databricks commands export.
---

## Export

### Synopsis

```bash
databricks-sync export --profile <profile> (--git-ssh-url | -g) <url>
databricks-sync export --profile <profile> (--local-git-path | -l) <path>
databricks-sync export --help
databricks-sync export --profile <profile> ((--git-ssh-url | -g) <url> | (--local-git-path | -l) <path>) [--branch <branch name> default: master] [(--ssh-key-path | -k) <path> default: ~/.ssh/id_rsa] [(--verbosity | -v) <level>] [--version <version>] [(--config-path | -c) <path>] [--dask] [--dry-run] [--tag <aws-tags | azure-tags>]
```

### Description

**export** creates a snapshot of the Databricks-native objects within a Databricks workspace then saves this state to a provided Git repository.

### Arguments

* `--profile` - The Databricks CLI connection profile for the  source workspace. For additional information, please see the Databricks Sync [Setup instructions](https://github.com/databrickslabs/databricks-sync/blob/master/docs/setup.md). If no profile was configured for the Databricks CLI during setup, then `DEFAULT` should be passed as the value.
* `--git-ssh-url` or `-g` - The URL of the remote git repo.
* `--local-git-path` or `-l` - The path of a local git repo.

### Options

* `--branch` - This is the git repo branch of the repo designated by `--git-ssh-url flag | --local-git-path`. If not given, the default branch is `master`.
* `--ssh-key-path` or `-k` - CLI connection profile to use. The default value is `~/.ssh/id_rsa`. This is equivalent to the `-i` switch when using `ssh`.
* `--verbosity` or `-v` - For logging, takes a value of `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`.
* `--version` - A version can be attached.
* `--config-path` or `-c` - This is the relative path (to the root directory of this repo) or the full path of the yaml file which is used to drive which objects are imported/exported.
* `--dask` - This is a flag to use [dask](https://docs.dask.org/en/latest/) to parallelize the process.
* `--dry-run` - This flag will log to console the actions but not commit to git remote state.

### Example

```bash
# Export state of objects associated with DEFAULT profile to the master branch of a remote GitHub repository
databricks-sync export --profile DEFAULT export -g git@github.com:USERNAME/REPOSITORY.git

# Export state of objects associated with test-workspace profile to a local repository on feature-213 branch
Databricks-sync export  --profile test-workspace -l /path/to/local/git/repo --branch feature-213

```

