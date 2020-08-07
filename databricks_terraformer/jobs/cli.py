import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.hcl.json_to_hcl import create_resource_from_dict
from databricks_terraformer.utils import handle_block, handle_map, normalize_identifier, prep_json
from databricks_terraformer.utils.git_handler import GitExportHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


def handle_libraries(resource_data, object, map):
    map_resource_data = {}
    log.debug(object[map])
    for key in object[map][0]:
        map_resource_data['library_' + key] = {'path': object[map][0][key], }

    resource_data[f"{map}"] = map_resource_data



@click.command(context_settings=CONTEXT_SETTINGS, help="Export Jobs.")
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
        "new_cluster": handle_block,
        "notebook_task": handle_block,
        "aws_attributes": handle_block,
        "spark_env_vars": handle_block,
        "autoscale": handle_block,
        "spark_submit_task": handle_block,
        "libraries": handle_libraries,
        "email_notifications": handle_map,
        "custom_tags": handle_map
    }
    ignore_attribute_key = {
        "created_time", "creator_user_name", "job_id"
    }
    required_attributes_key = {
        "max_concurrent_runs", "name"
    }

    if hcl:
        job_api = JobsApi(api_client)

        jobs = job_api.list_jobs()["jobs"]
        log.info(jobs)

        with GitExportHandler(git_ssh_url, "jobs", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for job in jobs:
                if not pattern_matches(job["settings"]["name"]):
                    log.debug(f"{job['settings']['name']} did not match pattern function {pattern_matches}")
                    continue
                log.debug(f"{job['settings']['name']} matched the pattern function {pattern_matches}")
                job_resource_data = prep_json(block_key_map, ignore_attribute_key, job['settings'], required_attributes_key)

                base_name = normalize_identifier(job['settings']['name'])
                name = "databricks_job"
                identifier = f"databricks_job-{base_name}"

                #need to escape quotes in the name.
                job_resource_data['name'] = job_resource_data['name'].replace('"','\\"')

                instance_job_hcl = create_resource_from_dict(name, identifier, job_resource_data, False)
                file_name_identifier = f"{identifier}.tf"
                gh.add_file(file_name_identifier, instance_job_hcl)
                log.debug(instance_job_hcl)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with Jobs.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def jobs_group():
    """
    Utility to interact with Databricks Jobs.
    """
    pass


jobs_group.add_command(export_cli, name="export")

# GIT_PYTHON_TRACE=full databricks-terraformer -v debug jobs export --hcl --profile demo
# GIT_PYTHON_TRACE=full databricks-terraformer -v debug jobs export --hcl --profile demo-aws -g git@github.com:stikkireddy/export-repo.git --dry-run --delete