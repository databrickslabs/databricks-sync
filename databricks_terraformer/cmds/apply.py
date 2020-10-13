from pathlib import Path

import click
from databricks_cli.configure.config import debug_option, profile_option
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS
from databricks_terraformer.cmds.config import git_url_option, ssh_key_option, inject_profile_as_env, \
    absolute_path_callback
from databricks_terraformer.sdk.sync.import_ import TerraformExecution

SUPPORT_IMPORTS = ['cluster_policy', 'dbfs_file', 'notebook', 'identity', 'instance_profile', 'instance_pool', 'secrets']


# TODO: Custom state back ends using aws environment variables
@click.command(context_settings=CONTEXT_SETTINGS, help="Import selected resources.")
@click.option("--plan", is_flag=True, help='This will generate the terraform plan to your infrastructure.')
@click.option("--apply", is_flag=True, help='This will apply the plan and will make modifications to your '
                                            'infrastructure.')
@click.option("--destroy", is_flag=True, help='Indicate whether you wish to destroy all the provisioned '
                                              'infrastructure.')
@click.option("--skip-refresh", is_flag=True, help='This is to determine whether you need to refresh remote state or not',
              default=False)
@click.option("--artifact-dir", required=True, type=click.Path(exists=True), callback=absolute_path_callback,
              help='Will be where the plan/state file be saved, required unless backend state is specified.')
@click.option("--revision", type=str, help='This is the git repo revision which can be a branch, commit, tag.')
@click.option('--databricks-object-type', type=click.Choice(SUPPORT_IMPORTS),
              multiple=True, default=SUPPORT_IMPORTS,
              help="This is the databricks object you wish to create a plan for. By default we will plan for all objects.")
@click.option("--backend-file", type=str,
              help='Please provide this as this is where your backend configuration at which your terraform file will be saved.')
@debug_option
@profile_option
@eat_exceptions
@git_url_option
@ssh_key_option
@inject_profile_as_env
def import_cli(git_ssh_url, databricks_object_type, plan, apply, backend_file, skip_refresh, destroy, revision, artifact_dir):
    te = TerraformExecution(
        git_ssh_url,
        revision=revision,
        folders=databricks_object_type,
        destroy=destroy,
        plan=plan,
        apply=apply,
        refresh=not skip_refresh,
        # Hard coded for now
        plan_location=Path(artifact_dir) / "plan.out",
        state_location=Path(artifact_dir) / "state.tfstate",
    )
    te.execute()