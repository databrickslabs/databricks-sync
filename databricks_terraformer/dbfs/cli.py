import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.sdk import ApiClient, DbfsService
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.config import git_url_option, ssh_key_option, delete_option, dry_run_option, tag_option
from databricks_terraformer.dbfs import get_file_contents
from databricks_terraformer.hcl import create_hcl_file
from databricks_terraformer.hcl.json_to_hcl import create_hcl_from_json, validate_hcl
from databricks_terraformer.utils import normalize_identifier
from databricks_terraformer.utils.git_handler import GitExportHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export DBFS files.")
@click.option("--hcl", is_flag=True, help='Will export the data as HCL.')
@click.option("--dbfs-path", required=True)
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
def export_cli(tag, dry_run, dbfs_path, delete, git_ssh_url, api_client: ApiClient, hcl, pattern_matches):
    if hcl:
        log.debug("this if debug")
        service = DbfsService(api_client)

        files = service.list(path=dbfs_path)['files']
        log.info(files)

        with GitExportHandler(git_ssh_url, "dbfs", delete_not_found=delete, dry_run=dry_run, tag=tag) as gh:
            for file in files:
                assert "path" in file
                assert "is_dir" in file
                assert "file_size" in file
                if file["is_dir"]:
                    continue
                base_name = file["path"]

                identifier = normalize_identifier(f"databricks_dbfs_file-{base_name}")
                dbfs_resource_data = {
                    "@expr:source": f'pathexpand("{identifier}")',
                    "@expr:content_b64_md5": f'md5(filebase64(pathexpand("{identifier}")))',
                    "path": file["path"],
                    "overwrite": True,
                    "mkdirs": True,
                    "validate_remote_file": True,
                }

                o_type = "resource"
                name = "databricks_dbfs_file"

                dbfs_file_hcl = create_hcl_from_json(o_type, name, identifier, dbfs_resource_data, False)

                processed_hcl_file = create_hcl_file(file['path'], api_client.url, dbfs_resource_data,
                                                     dbfs_file_hcl)

                gh.add_file(f"{identifier}.tf", processed_hcl_file)
                gh.add_file(f"files/{identifier}", get_file_contents(service, file["path"]))
                hcl_errors = validate_hcl(dbfs_file_hcl)
                if len(hcl_errors) > 0:
                    log.error(f"Identified error in the following HCL Config: {dbfs_file_hcl}")
                    log.error(hcl_errors)


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to interact with DBFS files.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@debug_option
@profile_option
@eat_exceptions
def dbfs_group():
    """
    Utility to interact with Databricks DBFS.
    """
    pass


dbfs_group.add_command(export_cli, name="export")

