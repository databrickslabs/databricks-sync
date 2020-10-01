import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.misc.cluster_policies import PolicyService
from databricks_terraformer.cmds.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.sdk.hcl import create_hcl_file
from databricks_terraformer.sdk.hcl.json_to_hcl import validate_hcl, create_resource_from_dict
from databricks_terraformer.sdk.utils import normalize_identifier
from databricks_terraformer.misc.utils import GitExportHandler
from databricks_terraformer.misc.utils import provide_pattern_func
from databricks_terraformer.cmds.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export cluster policies.")
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
def export_cli(tag, dry_run, delete, git_ssh_url, api_client: ApiClient, hcl, pattern_matches):
    if hcl is True:
        service = PolicyService(api_client)
        created_policy_list = []
        with GitExportHandler(git_ssh_url, "cluster_policies", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for policy in service.list_policies()["policies"]:
                assert "definition" in policy
                assert "name" in policy
                assert "policy_id" in policy
                if not pattern_matches(policy["name"]):
                    log.debug(f"{policy['name']} did not match pattern function {pattern_matches}")
                    continue
                log.debug(f"{policy['name']} matched the pattern function {pattern_matches}")
                cluster_policy_tf_dict = {
                    "@raw:definition": policy["definition"],
                    "name": policy["name"]
                }
                name = "databricks_cluster_policy"
                identifier = normalize_identifier(f"databricks_cluster_policy-{policy['name']}-{policy['policy_id']}")
                created_policy_list.append(identifier)
                policy_hcl = create_resource_from_dict(name, identifier, cluster_policy_tf_dict, False)
                file_name_identifier = f"{identifier}.tf"

                processed_hcl_file = create_hcl_file(policy['policy_id'], api_client.url, cluster_policy_tf_dict,
                                                     policy_hcl)

                gh.add_file(file_name_identifier, processed_hcl_file)
                hcl_errors = validate_hcl(policy_hcl)
                if len(hcl_errors) > 0:
                    log.error(f"Identified error in the following HCL Config: {policy_hcl}")
                    log.error(hcl_errors)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with clusters cluster policies.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def cluster_policies_group():
    """
    Utility to interact with Databricks clusters.
    """
    pass


cluster_policies_group.add_command(export_cli, name="export")
