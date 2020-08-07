import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient
from databricks_cli.secrets.api import SecretApi
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.hcl.json_to_hcl import create_resource_from_dict
from databricks_terraformer.utils import normalize_identifier, prep_json
from databricks_terraformer.utils.git_handler import GitExportHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Secrets for all Scopes.")
@click.option("--hcl", is_flag=True, help='Will export the data as HCL.')
@provide_pattern_func("pattern_matches")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
@git_url_option
@ssh_key_option
@delete_option
@dry_run_option
@tag_option
def export_cli(dry_run, tag, delete, git_ssh_url, api_client: ApiClient, hcl, pattern_matches):
    block_key_map = {
    }
    ignore_attribute_key = {
        "last_updated_timestamp"
    }
    required_attributes_key = {
        "key"
    }

    if hcl:
        secret_api = SecretApi(api_client)

        scopes = secret_api.list_scopes()["scopes"]
        log.info(scopes)

        with GitExportHandler(git_ssh_url, "secrets", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for scope in scopes:
                secrets = secret_api.list_secrets(scope["name"])["secrets"]
                log.info(secrets)

                for secret in secrets:
                    if not pattern_matches(secret["key"]):
                        log.debug(f"{secret['key']} did not match pattern function {pattern_matches}")
                        continue
                    log.debug(f"{secret['key']} matched the pattern function {pattern_matches}")
                    secret_resource_data = prep_json(block_key_map, ignore_attribute_key, secret, required_attributes_key)

                    base_name = normalize_identifier(secret["key"])
                    name = "databricks_secret"
                    identifier = f"databricks_secret-{base_name}"

                    secret_resource_data["scope"] = scope["name"]

                    secret_hcl = create_resource_from_dict(name, identifier, secret_resource_data, False)

                    file_name_identifier = f"{identifier}.tf"
                    gh.add_file(file_name_identifier, secret_hcl)
                    log.debug(secret_hcl)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Secrets.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def secrets_group():
    """
    Utility to interact with Databricks Secrets.
    """
    pass


secrets_group.add_command(export_cli, name="export")

# GIT_PYTHON_TRACE=full databricks-terraformer -v debug secrets export --hcl --profile demo
