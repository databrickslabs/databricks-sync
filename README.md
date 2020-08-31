# Databricks Sync
Use Databricks Sync to backup restore and sync Databricks workspaces

This package uses credentials from the [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)

Support Matrix for Import and Export Operations:

| component                    | export to hcl | workspace import |
| -----------------------------|---------------|------------------|
| User Objects                                                    |
| cluster policy               | supported     | supported        |
| cluster                      | unsupported   | unsupported      |
| dbfs file                    | supported     | supported        |
| instance pool                | supported     | supported        |
| instance profile             | supported     | supported        |
| job                          | supported     | unsupported      |
| notebook                     | supported     | supported        |
| -----------------------------|---------------|------------------|
| Administrator setup                                             |
| mws credentials              | unsupported   | unsupported      |
| mws networks                 | unsupported   | unsupported      |
| mws storage configurations   | unsupported   | unsupported      |
| mws workspaces               | unsupported   | unsupported      |
| aws s3 mount                 | unsupported   | unsupported      |
| azure adls gen1 mount        | unsupported   | unsupported      |
| azure adls gen2 mount        | unsupported   | unsupported      |
| azure blob mount             | unsupported   | unsupported      |
| secret                       | supported     | unsupported      |
| secret acl                   | supported     | supported        |
| secret scope                 | supported     | supported        |
| metastore tables             | unsupported   | unsupported      |
| metastore table ACLs         | unsupported   | unsupported      |
| -----------------------------|---------------|------------------|
| Users Management                                                |
| group                        | unsupported   | unsupported      |
| group instance profile       | unsupported   | unsupported      |
| group member                 | unsupported   | unsupported      |
| scim group                   | unsupported   | unsupported      |
| scim user                    | unsupported   | unsupported      |


# Databricks Terraform Provider

[![Build Status](https://travis-ci.org/databrickslabs/databricks-terraform.svg?branch=master)](https://travis-ci.org/databrickslabs/databricks-terraform)

## Quickstart: Building and Using the Provider

### Quick install

### Install using go 

Please note that there is a Makefile which contains all the commands you would need to run this project.

This code base to contribute to requires the following software:

* [golang 1.13.X](https://golang.org/dl/)
* [terraform v0.12.x](https://www.terraform.io/downloads.html)

To quickly install the binary please execute the following curl command in your shell.

```bash
$ curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins
```

* pip install -r requirements.txt

* run make command at the root of the project to build

The command should have moved the binary into your `~/.terraform.d/plugins` folder.

You can `ls` the previous directory to verify. 

To make sure everything is installed correctly please run the following commands:

Testing go installation:
```bash
$ go version 
go version go1.13.3 darwin/amd64
```

Testing terraform installation:
```bash
$ terraform --version
Terraform v0.12.19

Your version of Terraform is out of date! The latest version
is 0.12.24. You can update by downloading from https://www.terraform.io/downloads.html

```

Testing make installation:
```bash
$ make --version
GNU Make 3.81
Copyright (C) 2006  Free Software Foundation, Inc.
This is free software; see the source for copying conditions.
There is NO warranty; not even for MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.

This program built for i386-apple-darwin11.3.0


# Next steps:
* Add Terraform output to capture depedand ID (such as cluster policy ID for a cluster
* Export object permissions and import them to the target with the object
* Add examples for different scenarios:
    * Backup and Restore
    * CI/CD
    * Disaster Recovery Sync
    * Batch modification (will require Terraform Object Import support)
      

# Common commands

```shell

# make; GIT_PYTHON_TRACE=full databricks-terraformer cluster-policies export --hcl --profile demo-aws -g git@github.com:stikkireddy/test-demo-repo.git --dry-run --delete
# make; GIT_PYTHON_TRACE=full databricks-terraformer cluster-policies export --hcl --profile demo-aws -g git@github.com:stikkireddy/demo-test-repo.git --delete --dry-run
# make; GIT_PYTHON_TRACE=full databricks-terraformer notebooks export --hcl --notebook-path /Users/sri.tikkireddy@databricks.com/ --profile demo-aws -g git@github.com:stikkireddy/demo-test-repo.git --delete --dry-run
# make; GIT_PYTHON_TRACE=full databricks-terraformer dbfs export --hcl --dbfs-path dbfs:/databricks/init_scripts --profile demo-aws -g git@github.com:stikkireddy/test-demo-repo.git --delete --dry-run
# make; GIT_PYTHON_TRACE=full databricks-terraformer dbfs export --hcl --dbfs-path dbfs:/databricks/init_scripts --profile demo-aws -g git@github.com:stikkireddy/test-export-2.git --delete --dry-run
# make; GIT_PYTHON_TRACE=full databricks-terraformer import --profile azure-my-vnet -g git@github.com:stikkireddy/test-export-2.git --revision master --plan --artifact-dir tmp
make; GIT_PYTHON_TRACE=full databricks-terraformer import \
    --profile azure-my-vnet \
    -g git@github.com:stikkireddy/test-demo-repo.git \
    --revision 97275b43d55c7b108e88c7ad4621c003f39057f5 \
    --plan \
    --artifact-dir tmp \
    --apply \
    --backend-file tmp/backend.tf
```

## Commands to do local development

To do active development on your local laptop it will require the following:

1. Install go 1.13 or 1.14
2. Install terraform
3. Install databricks

```bash
$ make shared install
```

## Git instructions

This project only uses git with the ssh protocol so please generate rsa keys. Please follow these guides:

1. [Create ssh keys.](https://www.digitalocean.com/docs/droplets/how-to/add-ssh-keys/create-with-openssh/)
2. [Adding your keys to github for testing.](https://docs.github.com/en/enterprise/2.15/user/articles/adding-a-new-ssh-key-to-your-github-account)


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

Please use the following alias to use the tool. Where dbt -> databricks-terraformer. You can choose to call it something else.
This script creates 3 volume mounts with docker of which two are read only.
1. We mount $PWD or present working directory to /usr/src/databricks-terraformer as that is the working directory.
This allows you to manipulate files in the local working directory on your host machine.
2. The second mount is mounting ~/.databrickscfg to /root as that is the home directory of the container. 
This mount is read only.
3. The third mount is mounting ~/.ssh folder to the /root/.ssh folder. This is so the script can fetch your 
private keys in a read only fashion for accessing the git repository. This is also a read only mount.


```bash
$ alias dbt='docker run -it --rm --name docker-terraformer --env-file <(env | grep "ARM\|AWS") -v "$PWD":/usr/src/databricks-terraformer -v ~/.databrickscfg:/root/.databrickscfg:ro -v ~/.ssh:/root/.ssh:ro -w /usr/src/databricks-terraformer databricks-terraformer'
```

For example using dbt which is the alias above you should be able to do the following few commandss:

```bash
$ dbt --help
```

```bash
$ dbt import \
    --profile azure-my-vnet \
    -g <github ssh url> \
    --revision <git revision (commit, tag or branch)> \
    --plan \
    --artifact-dir tmp \
    --apply \
    --backend-file tmp/backend.tf
```
