from pathlib import Path

import click
from databricks_cli.configure.config import profile_option, debug_option, provide_api_client
from databricks_cli.sdk import ApiClient

from databricks_sync import CONTEXT_SETTINGS
from databricks_sync.cmds.config import git_url_option, ssh_key_option, dry_run_option, \
    dask_option, local_git_option, validate_git_params, config_path_option, handle_additional_debug, \
    wrap_with_user_agent, excel_report_option, inject_profile_as_env, branch_option
from databricks_sync.sdk.sync.export import ExportCoordinator


@click.command(context_settings=CONTEXT_SETTINGS, help="Export resource files.")
@config_path_option
@profile_option
@excel_report_option
@local_git_option
@git_url_option
@branch_option
# @eat_exceptions
@wrap_with_user_agent(provide_api_client)
@ssh_key_option
@dry_run_option
@inject_profile_as_env
@dask_option
@debug_option
@click.pass_context
def export_cli(ctx, dry_run, git_ssh_url, local_git_path, dask, config_path, api_client: ApiClient, branch, excel_report):
    # TODO: log the api client config and etc
    handle_additional_debug(ctx)
    validate_git_params(git_ssh_url, local_git_path)
    ExportCoordinator.export(api_client, Path(config_path), dask_mode=dask, dry_run=dry_run, git_ssh_url=git_ssh_url,
                             local_git_path=local_git_path, branch=branch, excel_report=excel_report)

