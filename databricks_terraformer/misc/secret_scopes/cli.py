import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient
from databricks_cli.secrets.api import SecretApi
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.cmds.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.sdk.hcl.json_to_hcl import create_resource_from_dict
from databricks_terraformer.misc.utils import prep_json
from databricks_terraformer.sdk.utils import normalize_identifier
from databricks_terraformer.misc.utils import GitExportHandler
from databricks_terraformer.misc.utils import provide_pattern_func
from databricks_terraformer.cmds.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Secret Scopes.")
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
    }
    required_attributes_key = {
        "name", "backend_type", "is_databricks_managed"
    }

    if hcl:
        secret_api = SecretApi(api_client)

        scopes = secret_api.list_scopes()["scopes"]
        log.info(scopes)

        with GitExportHandler(git_ssh_url, "secret_scopes", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for scope in scopes:
                if not pattern_matches(scope["name"]):
                    log.debug(f"{scope['name']} did not match pattern function {pattern_matches}")
                    continue
                log.debug(f"{scope['name']} matched the pattern function {pattern_matches}")
                scope_resource_data = prep_json(block_key_map, ignore_attribute_key, scope, required_attributes_key)

                base_name = normalize_identifier(scope["name"])
                name = "databricks_secret_scope"
                identifier = f"databricks_secret_scope-{base_name}"

                secret_scope_hcl = create_resource_from_dict(name, identifier, scope_resource_data, False)

                file_name_identifier = f"{identifier}.tf"
                gh.add_file(file_name_identifier, secret_scope_hcl)
                log.debug(secret_scope_hcl)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Secret Scopes.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def secret_scopes_group():
    """
    Utility to interact with Databricks Secret Scopes.
    """
    pass


secret_scopes_group.add_command(export_cli, name="export")

# GIT_PYTHON_TRACE=full databricks-terraformer -v debug secret-scopes export --hcl --profile demo
