---
title: "Introduction"
description: "A tool used to synchronize a source Databricks deployment with a target Databricks deployment. Useful for migration and Disaster Recovery."
lead: "A tool used to a synchronize source Databricks deployment with a target Databricks deployment. Useful for migration and Disaster Recovery."
date: 2020-10-06T08:48:57+00:00
lastmod: 2020-10-06T08:48:57+00:00
draft: false
images: ["solution-arch.png"]
menu:
  docs:
    parent: "getting-started"
weight: 100
toc: true
---

## Reference Architecture

{{< img src="solution-arch.png" alt="SolutionArchitecture"
    caption="<em>Databricks Sync Reference Architecture</em>" class="border-0" >}}

## Quickstart

### Basic Setup
* Configure [YAML file](https://github.com/databrickslabs/databricks-sync/blob/master/tests/integration_test.yaml).
* Export object permissions and import them to the target with the object
* TODO: Add examples for different scenarios:
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

1. Storing state in **aws s3**: [https://www.terraform.io/docs/backends/types/s3.html](https://www.terraform.io/docs/backends/types/s3.html)
2. Storing state in **azure blob**: [https://www.terraform.io/docs/backends/types/azurerm.html](https://www.terraform.io/docs/backends/types/azurerm.html)

### Docker instructions

{{< bootstrap-alert icon="💡" text=`<strong>Please be aware that the docker implementation will require you to have prior background in
using docker. The issues for this repository are only for reporting build failures; there wont be any direct support for docker issues.
If you need a stable environment to run this tool as a job please use Databricks Notebooks on a single node cluster.</strong>` >}}


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

| Component                                                                | Export | Import |
|--------------------------------------------------------------------------|--------|--------|
| **User Objects**                                                         |        |        |
| [Cluster Policies]({{< relref "workspace-cluster-policies.md" >}})       | ✅      | ✅      |
| [Clusters]({{< relref "workspace-clusters.md" >}})                       | ✅      | ✅      |
| [DBFS Files]({{< relref "workspace-dbfs-files.md" >}})                   | ✅      | ✅      |
| [Instance Pools]({{< relref "workspace-instance-pools.md" >}})           | ✅      | ✅      |
| [AWS Instance Profiles]({{< relref "workspace-identities.md" >}})        | ✅      | ✅      |
| [Jobs]({{< relref "workspace-jobs.md" >}})                               | ✅      | ✅      |
| [Notebooks]({{< relref "workspace-notebooks.md" >}})                     | ✅      | ✅      |
| [Global Init Scripts]({{< relref "workspace-global-init-scripts.md" >}}) | ✅      | ✅      |
| **Administrator Setup**                                                  |        |        |
| AWS S3 Mounts                                                            | ⬜️     | ⬜️     |
| Azure ADLS Gen1 Mounts                                                   | ⬜️     | ⬜️     |
| Azure ADLS Gen2 Mounts                                                   | ⬜️     | ⬜️     |
| Azure Blob Mount                                                         | ⬜️     | ⬜️     |
| [Secret]({{< relref "workspace-secrets.md" >}})                          | ✅      | ✅      |
| [Secret ACLs]({{< relref "workspace-secrets.md" >}})                     | ✅      | ✅      |
| [Secret Scopes]({{< relref "workspace-secrets.md" >}})                   | ✅      | ✅      |
| Metastore Tables                                                         | ⬜️     | ⬜️     |
| Metastore Table ACLs                                                     | ⬜️     | ⬜️     |
| **Users Management**                                                     |        |        |
| [Groups]({{< relref "workspace-identities.md" >}})                       | ✅      | ✅      |
| [Group AWS Instance Profiles]({{< relref "workspace-identities.md" >}})  | ✅      | ✅      |
| [Group Members]({{< relref "workspace-identities.md" >}})                | ✅      | ✅      |
| [Users]({{< relref "workspace-identities.md" >}})                        | ✅      | ✅      |
| [User AWS Instance Profiles]({{< relref "workspace-identities.md" >}})   | ✅      | ✅      |
| [Azure Service Principals]({{< relref "workspace-identities.md" >}})     | ✅      | ✅      |

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.
