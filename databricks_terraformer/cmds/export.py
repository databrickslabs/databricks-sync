from pathlib import Path

import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS
from databricks_terraformer.cmds.config import git_url_option, ssh_key_option, dry_run_option, config_file_option, \
    dask_option
from databricks_terraformer.sdk.sync.export import ExportCoordinator


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Notebook files.")
@debug_option
@profile_option
# @eat_exceptions
@provide_api_client
@git_url_option
@config_file_option
@ssh_key_option
@dry_run_option
@dask_option
def export_cli(dry_run, git_ssh_url, dask, config_path, api_client: ApiClient):
    ExportCoordinator.export(api_client, git_ssh_url, Path(config_path), dry_run, dask)