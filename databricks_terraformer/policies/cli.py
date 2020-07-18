import json
import os
import tempfile
from datetime import datetime

import click
import git
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.hcl.json_to_hcl import create_hcl_from_json
from databricks_terraformer.policies.policies_service import PolicyService
from databricks_terraformer.utils.git_handler import GitHandler
from databricks_terraformer.utils.patterns import pattern_option, provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export cluster policies.")
@click.option("--hcl", is_flag=True, help='Will export the data as HCL.')
@provide_pattern_func("pattern_matches")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def export_cli(api_client: ApiClient, hcl, pattern_matches):
    if hcl:
        service = PolicyService(api_client)
        created_policy_list = []
        with GitHandler("https://github.com/stikkireddy/export-repo", "cluster_policies", ignore_deletes=True) as gh:
            for policy in service.list_policies()["policies"]:
                assert "definition" in policy
                assert "name" in policy
                assert "policy_id" in policy
                if not pattern_matches(policy["name"]):
                    log.debug(f"{policy['name']} did not match pattern function {pattern_matches}")
                    continue
                log.debug(f"{policy['name']} matched the pattern function {pattern_matches}")
                cluster_policy_tf_dict = {
                    "definition": policy["definition"],
                    "name": policy["name"]
                }
                o_type = "resource"
                name = "databricks_cluster_policy"
                identifier = f"databricks_cluster_policy-{policy['name']}-{policy['policy_id']}"
                created_policy_list.append(identifier)
                policy_hcl = create_hcl_from_json(o_type, name, identifier, cluster_policy_tf_dict, False)

                file_name_identifier = f"{identifier}.tf"
                gh.add_file(file_name_identifier, policy_hcl)
                log.debug(policy_hcl)



# deleting 405E7E8E4A000029
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


# GIT_PYTHON_TRACE=full databricks-terraformer cluster-policies export --hcl --profile field-eng --pattern "AChu*"