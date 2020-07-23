import click
from databricks_cli.configure.config import debug_option, profile_option
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS
from databricks_terraformer.config import git_url_option, ssh_key_option, inject_profile_as_env, absolute_path_callback
from databricks_terraformer.utils.terraform import GitTFStage_V2


SUPPORT_IMPORTS = ['cluster_policies', 'dbfs', 'notebooks', 'instance_pools']

# TODO: Custom state back ends using aws environment variables
@click.command(context_settings=CONTEXT_SETTINGS, help="Import selected resources.")
@click.option("--plan", is_flag=True, help='This will generate the terraform plan to your infrastructure.')
@click.option("--apply", is_flag=True, help='This will apply the plan and will make modifications to your '
                                            'infrastructure.')
# TODO: Validate plan path by running a terraform show on the path and it should return a valid plan
@click.option("--custom-plan-path", required=False, default=None, type=click.Path(exists=True),
              callback=absolute_path_callback, help='This should point to a plan.out or a similar terraform plan file.')
@click.option("--artifact-dir", required=True, type=click.Path(exists=True), callback=absolute_path_callback,
              help='Will be where the plan/state file be saved, required unless backend state is specified.')
@click.option("--revision", type=str, help='This is the git repo revision which can be a branch, commit, tag.')
@click.option('--databricks-object-type', type=click.Choice(SUPPORT_IMPORTS),
              multiple=True, default=SUPPORT_IMPORTS,
              help="This is the databricks object you wish to create a plan for. By default we will plan for all objects.")
@click.option("--backend-file", type=click.Path(exists=True), callback=absolute_path_callback,
              help='Please provide this as this is where your backend configuration at which your terraform file will be saved.')
@debug_option
@profile_option
@eat_exceptions
@git_url_option
@ssh_key_option
@inject_profile_as_env
def import_cli(git_ssh_url, databricks_object_type, plan, apply, backend_file, custom_plan_path, revision, artifact_dir):
    with GitTFStage_V2(git_url=git_ssh_url, directories=databricks_object_type,
                       cur_ref=revision,
                       artifact_dir=artifact_dir, backend_file=backend_file) as tf:
        if plan is True:
            tf.plan()
        if apply is True and plan is not True and custom_plan_path is None:
            raise ValueError("plan option is not selected but apply is selected without providing plan path.")
        if apply is True:
            tf.apply(custom_plan_path)


@click.command(context_settings=CONTEXT_SETTINGS, help="Delete all or selected resources.")
@click.option("--plan", is_flag=True, help='This will generate the terraform plan to your infrastructure.')
@click.option("--apply", is_flag=True, help='This will apply the plan and will make modifications to your '
                                            'infrastructure.')
# TODO: Validate plan path by running a terraform show on the path and it should return a valid plan
@click.option("--custom-plan-path", required=False, default=None, type=click.Path(exists=True),
              callback=absolute_path_callback, help='This should point to a plan.out or a similar terraform plan file.')
@click.option("--artifact-dir", required=True, type=click.Path(exists=True), callback=absolute_path_callback,
              help='Will be where the plan/state file be saved, required unless backend state is specified.')
@click.option("--revision", type=str, help='This is the git repo revision which can be a branch, commit, tag.')
@click.option('--databricks-object-type', type=click.Choice(SUPPORT_IMPORTS),
              multiple=True, default=SUPPORT_IMPORTS,
              help="This is the databricks object you wish to create a delete plan for. By default we will plan deletes for all objects.")
@click.option("--backend-file", type=click.Path(exists=True), callback=absolute_path_callback,
              help='Please provide this as this is where your backend configuration at which your terraform file will be saved.')
@debug_option
@profile_option
@eat_exceptions
@git_url_option
@ssh_key_option
@inject_profile_as_env
def destroy_cli(git_ssh_url, databricks_object_type, plan, apply, backend_file, custom_plan_path, revision, artifact_dir):
    databricks_objects_for_delete = list(set(SUPPORT_IMPORTS) - set(databricks_object_type))
    with GitTFStage_V2(git_url=git_ssh_url, directories=databricks_objects_for_delete,
                       cur_ref=revision,
                       artifact_dir=artifact_dir, backend_file=backend_file) as tf:
        if plan is True:
            tf.plan()
        if apply is True and plan is not True and custom_plan_path is None:
            raise ValueError("plan option is not selected but apply is selected without providing plan path.")
        if apply is True:
            tf.apply(custom_plan_path)
