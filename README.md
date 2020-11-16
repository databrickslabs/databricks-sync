# Databricks Sync (dbSync)
Use Databricks Sync to backup, restore, and sync Databricks workspaces

High level use cases include:
- Disaster Recovery
- CI/CD
- Workspace migration

This package uses credentials from the [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

# Quick Install

0. Download: `$ git clone https://github.com/databrickslabs/terraform-provider-databricks.git`
1. Install Databricks Terraform provider: `$ curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins`
2. Ensure Terraform and Golang dependencies (install via the package manager of your choice):
2a. Verify `terraform` version 0.12.x: `$ terraform version`
2b. Verify `golang` version 1.13.x: `$ go version`
2c. Verify `make` version 3.81: `$ make --version`
3. Install other dependencies: `$ cd terraform-provider-databricks && pip install -r requirementx.txt`
3. Install this project: `$ make shared install`
4. Test that the installation succeeded: `databricks-terraformer --version`
5. (Recommended) Run the following to alias CLI to `dbsync` (for more details, see "Aliasing" below):
```bash
$ alias dbsync='docker run -it --rm --name docker-terraformer --env-file <(env | grep "ARM\|AWS") -v "$PWD":/usr/src/databricks-terraformer -v ~/.databrickscfg:/root/.databrickscfg:ro -v ~/.ssh:/root/.ssh:ro -w /usr/src/databricks-terraformer databricks-terraformer'
```

# Quickstart

## Next steps:
* Add Terraform output to capture dependent ID (such as cluster policy ID for a cluster)
* Export object permissions and import them to the target with the object
* Add examples for different scenarios:
    * Backup and Restore
    * CI/CD
    * Disaster Recovery Sync
    * Batch modification (will require Terraform Object Import support)
      

## Common commands

```bash
$ databricks-terraformer  export \
    --profile <db cli profile> \
    --git-ssh-url git@github.com:..../.....git \
    -c ....test.yaml 
 
optional flags:
    -v DEBUG
    --dry-run 
    --dask
    --branch # support new main name convention

$ GIT_PYTHON_TRACE=full databricks-terraformer import \
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

## Backend Instructions (Storing terraform state in azure blob or aws s3)

When importing you are able to store and manage your state using blob or s3. You can do this by using the `--backend-file`.
This `--backend-file` will take a file path to the back end file. You can name the file `backend.tf`. This backend file will use
azure blob or aws s3 to manage the state file. To authenticate to either you will use environment variables. 

Please use `ARM_SAS_TOKEN` or `ARM_ACCESS_KEY` for sas token and account access key respectively for azure blob.   
Please use `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for the key and secret for the s3 bucket. Please go the following links in 
regards to policies and permissions. If you want to make the region dynamic you can use `AWS_DEFAULT_REGION`.   

1. Storing state in aws s3: https://www.terraform.io/docs/backends/types/s3.html
2. Storing state in azure blob (only azure blob is support as it supports locking): https://www.terraform.io/docs/backends/types/azurerm.html

## Docker instructions

These set of instructions are to use docker to build and use the CLI. It avoids the need to have golang, 
Terraform, the databricks-terraform-provider to get this to run. If you do want to work on this tool please 
install the prior listed tools to get this to work. 

To install this tool please run the following command:

```bash
$ docker build -t databricks-terraformer:latest .
```

## Dependencies

This project requires the following environment dependencies:
* [terraform 0.13.x](https://www.terraform.io/downloads.html)
* [terraform-provider-databricks 0.2.x](https://registry.terraform.io/providers/databrickslabs/databricks/latest)


## Aliasing

How our alias command works:

This script creates 3 volume mounts with docker of which two are read only.
1. We mount `$PWD` or present working directory to `/usr/src/databricks-terraformer` as that is the working directory.
This allows you to manipulate files in the local working directory on your host machine.
2. The second mount is mounting `~/.databrickscfg` to `/root` as that is the home directory of the container. 
This mount is read only.
3. The third mount is mounting `~/.ssh` folder to the `/root/.ssh` folder. This is so the script can fetch your 
private keys in a read only fashion for accessing the git repository. This is also a read only mount.


## Support Matrix for Import and Export Operations:

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

## Project Description
Short description of project's purpose

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.


## Building the Project
Instructions for how to build the project

## Deploying / Installing the Project
Instructions for how to deploy the project, or install it

## Releasing the Project
Instructions for how to release a version of the project

## Using the Project
Simple examples on how to use the project