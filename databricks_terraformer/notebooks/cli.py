import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient, WorkspaceService
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.hcl import create_hcl_file
from databricks_terraformer.hcl.json_to_hcl import validate_hcl, create_resource_from_dict
from databricks_terraformer.notebooks import get_workspace_notebooks_recursive, get_content
from databricks_terraformer.utils import normalize_identifier
from databricks_terraformer.utils.git_handler import GitExportHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export Notebook files.")
@click.option("--hcl", is_flag=True, help='Will export the data as HCL.')
@click.option("--notebook-path", required=True)
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
def export_cli(tag, dry_run, notebook_path, delete, git_ssh_url, api_client: ApiClient, hcl, pattern_matches):
    if hcl:
        log.debug("this if debug")
        service = WorkspaceService(api_client)
        files = get_workspace_notebooks_recursive(service, notebook_path)
        with GitExportHandler(git_ssh_url, "notebooks", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for file in files:
                identifier = normalize_identifier(f"databricks_notebook-{file.path}")
                content = get_content(service, file.path)
                if content is None:
                    continue
                notebook_resource_data = {
                    "@expr:content": f'filebase64("{identifier}")',
                    "path": file.path,
                    "overwrite": True,
                    "mkdirs": True,
                    "language": file.language,
                    "format": "SOURCE",
                }
                name = "databricks_notebook"
                notebook_file_hcl = create_resource_from_dict(name, identifier, notebook_resource_data, False)
                processed_hcl_file = create_hcl_file(file.path, api_client.url, notebook_resource_data,
                                                     notebook_file_hcl)
                gh.add_file(f"{identifier}.tf", processed_hcl_file)
                gh.add_file(f"files/{identifier}", content)
                hcl_errors = validate_hcl(notebook_file_hcl)
                if len(hcl_errors) > 0:
                    log.error(f"Identified error in the following HCL Config: {notebook_file_hcl}")
                    log.error(hcl_errors)
        # with GitExportHandler(git_ssh_url, "dbfs", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
        # for file in files:
        # assert "path" in file
        # assert "is_dir" in file
        # assert "file_size" in file
        # if file["is_dir"]:
        #     continue
        # base_name = file["path"]
        #
        # identifier = normalize_identifier(f"databricks_notebook-{base_name}")
        # dbfs_resource_data = {
        #     "@expr:source": f'pathexpand("{identifier}")',
        #     "@expr:content_b64_md5": f'md5(filebase64(pathexpand("{identifier}")))',
        #     "path": file["path"],
        #     "overwrite": True,
        #     "mkdirs": True,
        #     "validate_remote_file": True,
        # }
        #
        # o_type = "resource"
        # name = "databricks_dbfs_file"
        #
        # dbfs_file_hcl = create_hcl_from_json(o_type, name, identifier, dbfs_resource_data, False)
        #
        # processed_hcl_file = create_hcl_file(file['path'], api_client.url, dbfs_resource_data,
        #                                      dbfs_file_hcl)
        # print(processed_hcl_file)
        # gh.add_file(f"{identifier}.tf", processed_hcl_file)
        # gh.add_file(f"files/{identifier}", get_file_contents(service, file["path"]))
        # hcl_errors = validate_hcl(dbfs_file_hcl)
        # if len(hcl_errors) > 0:
        #     log.error(f"Identified error in the following HCL Config: {dbfs_file_hcl}")
        #     log.error(hcl_errors)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with DBFS files.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def notebook_group():
    """
    Utility to interact with Databricks DBFS.
    """
    pass


notebook_group.add_command(export_cli, name="export")
