# databricks-terraformer
Use Terraformer to backup restore and sync Databricks workspaces


# Common commands

```shell

# make; GIT_PYTHON_TRACE=full databricks-terraformer cluster-policies export --hcl --profile demo-aws -g git@github.com:stikkireddy/test-demo-repo.git --dry-run --delete
# make; GIT_PYTHON_TRACE=full dbt cluster-policies export --hcl --profile demo-aws -g git@github.com:stikkireddy/demo-test-repo.git --delete --dry-run
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
    --apply
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

1. Create ssh keys: https://www.digitalocean.com/docs/droplets/how-to/add-ssh-keys/create-with-openssh/
2. Adding your keys to github for testing. https://docs.github.com/en/enterprise/2.15/user/articles/adding-a-new-ssh-key-to-your-github-account

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
$ alias dbt='docker run -it --rm --name docker-terraformer -v "$PWD":/usr/src/databricks-terraformer -v ~/.databrickscfg:/root/.databrickscfg:ro -v ~/.ssh:/root/.ssh:ro -w /usr/src/databricks-terraformer databricks-terraformer'
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
    --apply
```