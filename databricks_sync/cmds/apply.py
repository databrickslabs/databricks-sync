from pathlib import Path

import click
from databricks_cli.configure.config import debug_option, provide_api_client
from databricks_cli.configure.config import profile_option
from databricks_cli.sdk import ApiClient

from databricks_sync import CONTEXT_SETTINGS
from databricks_sync.cmds.config import git_url_option, ssh_key_option, inject_profile_as_env, \
    absolute_path_callback, local_git_option, validate_git_params, handle_additional_debug, \
    wrap_with_user_agent
from databricks_sync.sdk.sync.constants import GeneratorCatalog
from databricks_sync.sdk.sync.import_ import TerraformExecution

SUPPORTED_IMPORTS = [
    GeneratorCatalog.CLUSTER_POLICY,
    GeneratorCatalog.DBFS_FILE,
    GeneratorCatalog.NOTEBOOK,
    GeneratorCatalog.IDENTITY,
    GeneratorCatalog.INSTANCE_POOL,
    GeneratorCatalog.INSTANCE_PROFILE,
    GeneratorCatalog.SECRETS,
    GeneratorCatalog.CLUSTER,
    GeneratorCatalog.JOB,
    GeneratorCatalog.GLOBAL_INIT_SCRIPT
]


# TODO: Custom state back ends using aws environment variables
@click.command(context_settings=CONTEXT_SETTINGS, help="Import selected resources.")
@click.option("--plan", is_flag=True, help='This will generate the terraform plan to your infrastructure.')
@click.option("--apply", is_flag=True, help='This will apply the plan and will make modifications to your '
                                            'infrastructure.')
@click.option("--destroy", is_flag=True, help='Indicate whether you wish to destroy all the provisioned '
                                              'infrastructure.')
@click.option("--skip-refresh", is_flag=True,
              help='This is to determine whether you need to refresh remote state or not',
              default=False)
@click.option("--artifact-dir", required=True, type=click.Path(exists=True), callback=absolute_path_callback,
              help='Will be where the plan/state file be saved, required unless backend state is specified.')
@click.option("--revision", type=str, help='This is the git repo revision which can be a branch, commit, tag.')
@click.option("--branch", type=str, help='This is the git repo branch.', default="master")
@click.option('--databricks-object-type', type=click.Choice(SUPPORTED_IMPORTS),
              multiple=True, default=SUPPORTED_IMPORTS,
              help="This is the databricks object you wish to create a plan for. By default we will plan for "
                   "all objects.")
@click.option("--backend-file", type=click.Path(exists=True, resolve_path=True),
              help='Please provide this as this is where your backend configuration at which your terraform file '
                   'will be saved.')
@debug_option
@profile_option
# @eat_exceptions
@local_git_option
@wrap_with_user_agent(provide_api_client)
@git_url_option
@ssh_key_option
@inject_profile_as_env
@click.pass_context
def import_cli(ctx, git_ssh_url, local_git_path, databricks_object_type, plan, apply, backend_file, skip_refresh,
               destroy,
               revision,
               artifact_dir, api_client: ApiClient, branch):
    # TODO: log the api client config and etc
    handle_additional_debug(ctx)
    validate_git_params(git_ssh_url, local_git_path)
    back_end_json = Path(backend_file) if backend_file is not None else None
    te = TerraformExecution(folders=databricks_object_type, refresh=not skip_refresh, revision=revision, plan=plan,
                            plan_location=Path(artifact_dir) / "plan.out",
                            local_state_location=Path(artifact_dir) / "state.tfstate", apply=apply, destroy=destroy,
                            git_ssh_url=git_ssh_url, local_git_path=local_git_path, api_client=api_client,
                            branch=branch, post_import_shutdown=True, back_end_json=back_end_json)
    te.execute()
