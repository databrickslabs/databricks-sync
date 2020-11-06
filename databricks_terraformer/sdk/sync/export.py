import tempfile
from pathlib import Path
from typing import Optional

from databricks_cli.sdk import ApiClient

from databricks_terraformer import log
from databricks_terraformer.cmds.apply import SUPPORTED_IMPORTS
from databricks_terraformer.sdk.config import export_config
from databricks_terraformer.sdk.generators.factory import GeneratorFactory
from databricks_terraformer.sdk.git_handler import GitHandler, LocalGitHandler, RemoteGitHandler
from databricks_terraformer.sdk.pipeline import ExportFileUtils, Pipeline
from databricks_terraformer.sdk.sync.import_ import TerraformExecution


class ExportCoordinator:

    @staticmethod
    def get_git_handler(local_git_path: Optional[str], git_ssh_url: Optional[str], tmp_dir: tempfile.TemporaryDirectory,
                        branch="master") \
            -> (GitHandler, Path):
        assert any([local_git_path, git_ssh_url]) is True, "atleast local git path or git ssh url should be provided " \
                                                           "otherwise if both are provided it will use local git path"
        # Local Git is prioritized and if it is used then no tmpdir needed
        if local_git_path is not None:
            return LocalGitHandler(Path(local_git_path), delete_directory=Path(ExportFileUtils.BASE_DIRECTORY)), \
                   Path(local_git_path)
        else:
            return RemoteGitHandler(git_ssh_url, Path(tmp_dir.name),
                                    delete_directory=Path(ExportFileUtils.BASE_DIRECTORY), branch=branch), Path(
                tmp_dir.name)

    @staticmethod
    def export(api_client: ApiClient, yaml_file_path: Path, dask_mode: bool = False, dry_run: bool = False,
               git_ssh_url: str = None, local_git_path=None, branch="master"):
        client = None
        if dask_mode is True:
            from distributed import Client
            client = Client(processes=True)
        tmp_dir = tempfile.TemporaryDirectory()
        try:
            geh, base_path = ExportCoordinator.get_git_handler(local_git_path, git_ssh_url, tmp_dir, branch=branch)
            export_config.set_from_yaml(
                yaml_file_path)

            generator_defaults = {
                "api_client": api_client,
                "base_path": base_path
            }
            export_objects = export_config.objects

            if export_objects is not None:
                generator_factory = GeneratorFactory.factory()
                generators = [generator_factory.make_generator(object_name, {**generator_defaults, **object_data})
                              for object_name, object_data in export_objects.items()]

                exp = Pipeline(generators,
                               base_path=base_path,
                               dask_client=client)
                exp.wire()
                exp.run()

            if dry_run is False:
                geh.stage_changes()
                changes = geh.get_changes("exports")
                tf_var_changes = geh.get_changes("terraform.tfvars")
                shell_env_changes = geh.get_changes("variables_env.sh")
                if any([changes is not None for changes in [changes, tf_var_changes, shell_env_changes]]):
                    geh.commit_and_push()
                else:
                    log.info("No changes found.")

            # We should run validate in either case
            te = TerraformExecution(
                SUPPORTED_IMPORTS,
                refresh=False,
                plan=False,
                apply=False,
                destroy=False,
                local_git_path=base_path,
                api_client=api_client,
                branch=branch,
            )
            te.execute()

        finally:
            tmp_dir.cleanup()
