from pathlib import Path

import click
from databricks_cli.configure.config import debug_option, provide_api_client
from databricks_cli.configure.config import profile_option
from databricks_cli.sdk import ApiClient

from databricks_sync import CONTEXT_SETTINGS
from databricks_sync.cmds.config import git_url_option, ssh_key_option, inject_profile_as_env, \
    local_git_option, validate_git_params, handle_additional_debug, \
    wrap_with_user_agent, branch_option, revision_option, backend_file_option, \
    databricks_object_type_option
from databricks_sync.sdk.sync.import_ import TerraformExecution

# TODO: Custom state back ends using aws environment variables
@click.command(context_settings=CONTEXT_SETTINGS, help="Run custom commands on the resources.")
@databricks_object_type_option
@profile_option
@click.option("--command", type=str, help='This is the raw command you want to run.', multiple=True)
@backend_file_option
@local_git_option
@git_url_option
@revision_option
@branch_option
# @eat_exceptions
@wrap_with_user_agent(provide_api_client)
@ssh_key_option
@debug_option
@inject_profile_as_env
@click.pass_context
def triage_cli(ctx, git_ssh_url, local_git_path, databricks_object_type, backend_file,
               revision,
               api_client: ApiClient, branch, command):
    # TODO: log the api client config and etc
    handle_additional_debug(ctx)
    validate_git_params(git_ssh_url, local_git_path)
    back_end_json = Path(backend_file) if backend_file is not None else None
    te = TerraformExecution(folders=databricks_object_type, refresh=False, revision=revision, plan=False,
                            plan_location=Path("/tmp/test") / "plan.out",
                            local_state_location=Path("/tmp/test") / "state.tfstate", apply=False, destroy=False,
                            git_ssh_url=git_ssh_url, local_git_path=local_git_path, api_client=api_client,
                            branch=branch, post_import_shutdown=True, back_end_json=back_end_json)
    te.execute(debug_commands=command)
