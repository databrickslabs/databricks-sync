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
    }
    required_attributes_key = {
        "principal", "permission"
    }

    if hcl:
        secret_api = SecretApi(api_client)

        scopes = secret_api.list_scopes()["scopes"]
        log.info(scopes)

        with GitExportHandler(git_ssh_url, "secret_acls", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for scope in scopes:
                acls = secret_api.list_acls(scope["name"])["items"]
                log.info(acls)

                for acl in acls:
                    acl_resource_data = prep_json(block_key_map, ignore_attribute_key, acl, required_attributes_key)

                    base_name = normalize_identifier(acl["principal"])
                    name = "databricks_secret_acl"
                    identifier = f"databricks_secret_acl-{base_name}"

                    acl_resource_data["scope"] = scope["name"]

                    acl_hcl = create_resource_from_dict(name, identifier, acl_resource_data, False)

                    file_name_identifier = f"{identifier}.tf"
                    gh.add_file(file_name_identifier, acl_hcl)
                    log.debug(acl_hcl)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Secret ACLs.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def secret_acls_group():
    """
    Utility to interact with Databricks Secret ACLs.
    """
    pass


secret_acls_group.add_command(export_cli, name="export")

# GIT_PYTHON_TRACE=full databricks-terraformer -v debug secret-acls export --hcl --profile demo
