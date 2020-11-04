# Integration Testing

The `run.sh` script will setup the required resources in specific clouds, if they are defined in `main.tf` and then pass these as environment variables to the `golang` integration tests. You can create `require_env` files with one line per environment variable and expect to have those definitely set for integration test run because of validation performed inside of it.

# Destruction

Bu default, we don't encourage creation/destruction of infrastructure multiple times per day, because in some cases it may take up to 30 minutes just to boot everything up. Therefore only `--destroy` flag in `run.sh` will trigger `terraform destroy -auto-approve`. `make test-*` tasks won't explicitly request destruction by default. 

# Conventions

* `azcli` - Azure authenticated with `az login` command. No `require_env` file needed. By far, the simplest way to develop provider's functionality.
* `OWNER` is variable name, that holds your email address. It's propagated down to all resourced on the cloud.
* One must aim to write integration tests that will run on all clouds without causing panic under any circumstance.

# Development loop

```bash
# keep credentials outside of repository
az login

# create azure environment
make azure-create
```
