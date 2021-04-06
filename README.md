# Databricks Sync (dbSync)

![Reference Architecture for Databricks-Sync](docs/solution-arch.png?raw=true)

## Introduction

Databricks Sync is an object synchronization tool to backup, restore, and sync Databricks workspaces.

High level use cases include:
- Disaster Recovery
- CI/CD
- Workspace migration

This package uses credentials from the [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

## Table of Contents

1. [Introduction](https://github.com/databrickslabs/databricks-sync#Introduction)
2. [Documentation](https://github.com/databrickslabs/databricks-sync/blob/master/docs)
   * [Setup](https://github.com/databrickslabs/databricks-sync/blob/master/docs/setup.md)
   * [Usage](https://github.com/databrickslabs/databricks-sync/blob/master/docs/usage.md)
   * [FAQ](https://github.com/databrickslabs/databricks-sync/blob/master/docs/faq.md)
   * [Contributing](https://github.com/databrickslabs/databricks-sync/blob/master/docs/contributing.md)
3. [Quickstart](https://github.com/databrickslabs/databricks-sync#Quickstart)
   * [Next Steps](https://github.com/databrickslabs/databricks-sync#next-steps)
   * [Common Commands](https://github.com/databrickslabs/databricks-sync#common-commands)
   * [Backend Instructions](https://github.com/databrickslabs/databricks-sync#backend-instructions-storing-terraform-state-in-azure-blob-or-aws-s3)
   * [Docker Instructions](https://github.com/databrickslabs/databricks-sync#docker-instructions)
   * [Aliasing](https://github.com/databrickslabs/databricks-sync#aliasing)
   * [Support Matrix for Import and Export Operations](https://github.com/databrickslabs/databricks-sync#support-matrix-for-import-and-export-operations)
4. [Project Support](https://github.com/databrickslabs/databricks-sync#project-support)
5. [Building the Project](https://github.com/databrickslabs/databricks-sync#building-the-project)
6. [Deploying / Installing the Project](https://github.com/databrickslabs/databricks-sync#deploying--installing-the-project)
7. [Releasing the Project](https://github.com/databrickslabs/databricks-sync#releasing-the-project)
8. [Using the Project](https://github.com/databrickslabs/databricks-sync#using-the-project)

## Documentation

See the [Databricks Sync Documentation](https://github.com/databrickslabs/databricks-sync/blob/master/docs) for information.

[Instructions to install Databricks Sync](https://github.com/databrickslabs/databricks-sync/blob/master/docs/setup.md) can be found here.

## Quickstart

### Next steps:
* Configure [YAML file](https://github.com/databrickslabs/databricks-sync/blob/master/tests/integration_test.yaml).
* Export object permissions and import them to the target with the object
* Add examples for different scenarios:
  * Backup and Restore
  * CI/CD
  * Disaster Recovery Sync
  * Batch modification (will require Terraform Object Import support)


### Common commands

```bash
$ databricks-sync init my-export-config

$ databricks-sync  export \
    --profile <db cli profile> \
    --git-ssh-url git@github.com:..../.....git \
    -c ....test.yaml

optional flags:
    -v DEBUG
    --dry-run
    --dask
    --branch # support new main name convention

$ GIT_PYTHON_TRACE=full databricks-sync import \
    -g git@github.com:.../....git \
    --profile dr_tagert \
    --databricks-object-type cluster_policy \
    --artifact-dir ..../dir \
    --plan \
    --skip-refresh \
    --revision ....
```

Control the databricks provider version by using:

```
export DATABRICKS_TERRAFORM_PROVIDER_VERSION="<version here>"
```

### Backend Instructions (Storing terraform state in azure blob or aws s3)

When importing you are able to store and manage your state using blob or s3. You can do this by using the `--backend-file`.
This `--backend-file` will take a file path to the back end file. You can name the file `backend.tf`. This backend file will use
azure blob or aws s3 to manage the state file. To authenticate to either you will use environment variables.

Please use `ARM_SAS_TOKEN` or `ARM_ACCESS_KEY` for sas token and account access key respectively for azure blob.
Please use `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for the key and secret for the s3 bucket. Please go the following links in
regards to policies and permissions. If you want to make the region dynamic you can use `AWS_DEFAULT_REGION`.

1. Storing state in aws s3: https://www.terraform.io/docs/backends/types/s3.html
2. Storing state in azure blob (only azure blob is support as it supports locking): https://www.terraform.io/docs/backends/types/azurerm.html

### Docker instructions

These set of instructions are to use docker to build and use the CLI. It avoids the need to have golang,
Terraform, the databricks-terraform-provider to get this to run. If you do want to work on this tool please
install the prior listed tools to get this to work.

To install this tool please run the following command:

```bash
$ docker build -t databricks-sync:latest .
```


#### Aliasing

How our alias command works:

This script creates 3 volume mounts with docker of which two are read only.
1. We mount `$PWD` or present working directory to `/usr/src/databricks-sync` as that is the working directory.
This allows you to manipulate files in the local working directory on your host machine.
2. The second mount is mounting `~/.databrickscfg` to `/root` as that is the home directory of the container.
This mount is read only.
3. The third mount is mounting `~/.ssh` folder to the `/root/.ssh` folder. This is so the script can fetch your
private keys in a read only fashion for accessing the git repository. This is also a read only mount.

```bash
alias dbt='docker run -it --rm --name docker-databricks-sync --env-file <(env | grep -e "[ARM|TF_VAR]") -v "$PWD":/usr/src/databricks-sync -v ~/.databrickscfg:/root/.databrickscfg:ro -v ~/.ssh:/root/.ssh:ro -w /usr/src/databricks-sync databricks-sync'
```

### Support Matrix for Import and Export Operations:

| Component                    | Export to HCL | Import to Workspace |Comments     |
| -----------------------------|---------------|---------------------|-------------|
| | **User Objects** |
| cluster policy               | ✅           |  ✅              | |
| cluster                      |  ✅            | ✅               | |
| dbfs file                    |  ✅           |  ✅              | |
| instance pool                |  ✅           |  ✅              | |
| instance profile             |  ✅           |  ✅              | |
| job                          |  ✅           |  ✅               | |
| notebook                     |  ✅           |  ✅              | |
| | **Administrator Setup** |
| aws s3 mount                 | ⬜️            | ⬜️               | |
| azure adls gen1 mount        | ⬜️            | ⬜️               | |
| azure adls gen2 mount        | ⬜️            | ⬜️               | |
| azure blob mount             | ⬜️            | ⬜️               | |
| secret                       |  ✅           |  ✅               | |
| secret acl                   |  ✅           |  ✅              | |
| secret scope                 |  ✅           |  ✅              | |
| metastore tables             | ⬜️            | ⬜️               | |
| metastore table ACLs         | ⬜️            | ⬜️               | |
| | **Users Management** |
| group                        |  ✅            |  ✅               | |
| group instance profile       |  ✅            |  ✅               | |
| group member                 |  ✅            |  ✅               | |
| scim user                    |  ✅            |  ✅               | |

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.
