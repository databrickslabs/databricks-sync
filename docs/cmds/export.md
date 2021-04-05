## Export

### Synopsis

```bash
databricks-sync export [--profile DATABRICKS_PROFILE_NAME default="DEFAULT"] {-l, --local-git-path PATH | -g --git-ssh-url REPO_URL} [--branch BRANCH_NAME] -c, --config-path PATH [-k, --ssh-key-path PATH default="~/.ssh/id_rsa"] [--dask] [--dry-run] [--debug] [--excel-report]
databricks-sync export -h, --help
```

### Description

`export` creates a snapshot of the Databricks-native objects within a Databricks workspace then saves this state to a provided Git repository.

### Options

* `--profile DATABRICKS_PROFILE_NAME` - The Databricks CLI connection profile for the  source workspace. For additional information, please see the Databricks Sync [Setup instructions](https://github.com/databrickslabs/databricks-sync/blob/master/docs/setup.md). If no profile was configured for the Databricks CLI during setup, then `DEFAULT` should be passed as the value.
* `-l, --local-git-path PATH` - The path of a local git repo. Cannot be supplied in conjunction with `-g, --git-ssh-url REPO_URL`.
* `-g, --git-ssh-url REPO_URL` - The URL of the remote git repo. Cannot be supplied in conjunction with `-l | --local-git-path`.
* `--branch BRANCH_NAME` - This is the git repo branch of the repo designated by `--git-ssh-url flag | --local-git-path`. If not given, the default branch is `master`.
* `-c, --config-path PATH` - This is the relative path (to the root directory of this repo) or the full path of the yaml file which is used to drive which objects are imported/exported.
* `-k, --ssh-key-path PATH` - CLI connection profile to use. The default value is `~/.ssh/id_rsa`. This is equivalent to the `-i` switch when using `ssh`.
* `--dask` - This is a flag to use [dask](https://docs.dask.org/en/latest/) to parallelize the process.
* `--dry-run` - This flag will log to console the actions but not commit to git remote state.
* `--debug` - Debug Mode. Shows full stack trace on error.
* `--excel-report` - This will export the full reporting into an excel (.xlsx) file.
* `-h, --help` - Shows Usage, Options, and Arguments then exits.

### Example

```bash
# Run export in local git
databricks-sync -v debug export --profile DATABRICKS_EXPORT_PROFILE_NAME -l ~/DBFS_LOCAL_REPO_NAME -c DBFS_EXPORT_CONFIG_FILENAME.yaml --dask

# Run export with remote git repo
databricks-sync -v debug export --profile DATABRICKS_EXPORT_PROFILE_NAME -g REPO_URL -c DEFAULT_EXPORT_CONFIG_FILENAME.yaml --branch main --dask
```

