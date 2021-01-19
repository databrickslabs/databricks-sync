from pathlib import Path

import click
from click import pass_context
from databricks_cli.configure.config import profile_option, provide_api_client, debug_option
from databricks_cli.sdk import ApiClient

from databricks_terraformer import CONTEXT_SETTINGS
from databricks_terraformer.cmds.config import git_url_option, ssh_key_option, dry_run_option, \
    dask_option, local_git_option, validate_git_params, config_path_option, handle_additional_debug, add_user_agent
from databricks_terraformer.sdk.sync.export import ExportCoordinator


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Notebook files.")
@click.option("--branch", type=str, help='This is the git repo branch.', default="master")
@debug_option
@profile_option
# @eat_exceptions
@provide_api_client
@add_user_agent
@config_path_option
@local_git_option
@git_url_option
@ssh_key_option
@dry_run_option
@dask_option
@pass_context
def export_cli(ctx, dry_run, git_ssh_url, local_git_path, dask, config_path, api_client: ApiClient, branch):
    # TODO: log the api client config and etc
    handle_additional_debug(ctx)
    validate_git_params(git_ssh_url, local_git_path)
    ExportCoordinator.export(api_client, Path(config_path), dask_mode=dask, dry_run=dry_run, git_ssh_url=git_ssh_url,
                             local_git_path=local_git_path, branch=branch)

