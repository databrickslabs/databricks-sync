import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.hcl.json_to_hcl import create_resource_from_dict
from databricks_terraformer.utils import handle_block, handle_map, normalize_identifier, prep_json
from databricks_terraformer.utils.git_handler import GitExportHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Instance Pools.")
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
        "aws_attributes": handle_block,
        "disk_spec": handle_block,
        "custom_tags": handle_map
    }
    ignore_attribute_key = {
        "stats", "state", "status", "default_tags", "instance_pool_id"
    }
    required_attributes_key = {
        "instance_pool_name", "min_idle_instances", "idle_instance_autotermination_minutes", "node_type_id"
    }

    if hcl:
        pool_api = InstancePoolsApi(api_client)

        pools = pool_api.list_instance_pools()["instance_pools"]
        log.info(pools)

        with GitExportHandler(git_ssh_url, "instance_pools", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for pool in pools:
                if not pattern_matches(pool["instance_pool_name"]):
                    log.debug(f"{pool['instance_pool_name']} did not match pattern function {pattern_matches}")
                    continue
                log.debug(f"{pool['instance_pool_name']} matched the pattern function {pattern_matches}")
                pool_resource_data = prep_json(block_key_map, ignore_attribute_key, pool, required_attributes_key)

                base_name = normalize_identifier(pool["instance_pool_name"])
                name = "databricks_instance_pool"
                identifier = f"databricks_instance_pool-{base_name}"

                instance_pool_hcl = create_resource_from_dict(name, identifier, pool_resource_data, False)

                file_name_identifier = f"{identifier}.tf"
                gh.add_file(file_name_identifier, instance_pool_hcl)
                log.debug(instance_pool_hcl)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Instance Pools.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def instance_pools_group():
    """
    Utility to interact with Databricks Instance Pools.
    """
    pass


instance_pools_group.add_command(export_cli, name="export")

# GIT_PYTHON_TRACE=full databricks-terraformer -v debug instance-pools export --hcl --profile demo
# GIT_PYTHON_TRACE=full databricks-terraformer -v debug instance-pools export --hcl --profile demo-aws -g git@github.com:stikkireddy/export-repo.git --dry-run --delete