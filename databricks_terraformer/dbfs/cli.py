import click
from databricks_cli.configure.config import debug_option, profile_option, provide_api_client
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.sdk import ApiClient, DbfsService
from databricks_cli.utils import eat_exceptions

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.hcl.json_to_hcl import create_hcl_from_json
from databricks_terraformer.utils.git_handler import GitHandler
from databricks_terraformer.utils.patterns import provide_pattern_func
from databricks_terraformer.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS, help="Export DBFS files.")
@click.option("--hcl", is_flag=True, help='Will export the data as HCL.')
@provide_pattern_func("pattern_matches")
@debug_option
@profile_option
@eat_exceptions
@provide_api_client
def export_cli(api_client: ApiClient, hcl, pattern_matches):
    if hcl:
        log.debug("this if debug")
        service = DbfsService(api_client)

        files = service.list(path="dbfs:/databricks/init_scripts")['files']
        log.info(files)

        #with GitHandler("git@github.com:itaiw/export-repo.git", "dbfs", ignore_deletes=True) as gh:
        for file in files:
            assert "path" in file
            assert "is_dir" in file
            assert "file_size" in file

            base_name = file["path"].replace(".","_").replace("/","_")

            print(f'${{pathexpand("{file["path"]}")}}')
            print(f'${{md5(filebase64(pathexpand("{file["path"]}"}}')

            dbfs_resource_data = {
                "source": f'${{pathexpand("{file["path"]}")}}',
                "content_b64_md5": f'${{md5(filebase64(pathexpand("{file["path"]}"}}',
                "path": file["path"],
                "overwrite": True,
                "mkdirs": True,
                "validate_remote_file": True,
            }

            o_type = "resource"
            name = "databricks_dbfs_file"
            identifier = f"databricks_dbfs_file-{base_name}"
            policy_hcl = create_hcl_from_json(o_type, name, identifier, dbfs_resource_data, False)

            print(policy_hcl)



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


dbfs_group.add_command(export_cli,name="export")