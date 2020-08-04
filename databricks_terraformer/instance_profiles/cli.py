import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.hcl.json_to_hcl import create_hcl_from_json
from databricks_terraformer.utils import handle_block, handle_map, normalize_identifier, prep_json
from databricks_terraformer.utils.git_handler import GitExportHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Instance Profiles.")
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
        "instance_profile_arn"
    }

    if hcl:
        _data = {}
        headers = None
        profiles = api_client.perform_query('GET', '/instance-profiles/list', data=_data, headers=headers)["instance_profiles"]
        log.info(profiles)

        with GitExportHandler(git_ssh_url, "instance_profiles", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for profile in profiles:
                if not pattern_matches(profile["instance_profile_arn"]):
                    log.debug(f"{profile['instance_profile_arn']} did not match pattern function {pattern_matches}")
                    continue
                log.debug(f"{profile['instance_profile_arn']} matched the pattern function {pattern_matches}")
                profile_resource_data = prep_json(block_key_map, ignore_attribute_key, profile, required_attributes_key)

                base_name = normalize_identifier(profile["instance_profile_arn"])
                o_type = "resource"
                name = "databricks_instance_profile"
                identifier = f"databricks_instance_profile-{base_name}"

                #Force validation. If we import it, we might as well be able to use it
                profile_resource_data["skip_validation"] = False
                instance_profile_hcl = create_hcl_from_json(o_type, name, identifier, profile_resource_data, False)

                file_name_identifier = f"{identifier}.tf"
                gh.add_file(file_name_identifier, instance_profile_hcl)
                log.debug(instance_profile_hcl)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Instance Profiles.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def instance_profiles_group():
    """
    Utility to interact with Databricks Instance Profiles.
    """
    pass


instance_profiles_group.add_command(export_cli, name="export")

# GIT_PYTHON_TRACE=full databricks-terraformer -v debug instance-profiles export --hcl --profile demo
