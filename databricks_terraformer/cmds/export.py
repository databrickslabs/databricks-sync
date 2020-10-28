from pathlib import Path

import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient

from databricks_terraformer import CONTEXT_SETTINGS
from databricks_terraformer.cmds.config import git_url_option, ssh_key_option, dry_run_option, config_path_option, \
    dask_option, local_git_option, validate_git_params
from databricks_terraformer.sdk.sync.export import ExportCoordinator


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Notebook files.")
@debug_option
@profile_option
# @eat_exceptions
@provide_api_client
@config_path_option
@local_git_option
@git_url_option
@ssh_key_option
@dry_run_option
@dask_option
def export_cli(dry_run, git_ssh_url, local_git_path, dask, config_path, api_client: ApiClient):
    validate_git_params(git_ssh_url, local_git_path)
    ExportCoordinator.export(api_client, Path(config_path), dask_mode=dask, dry_run=dry_run, git_ssh_url=git_ssh_url,
                             local_git_path=local_git_path)
