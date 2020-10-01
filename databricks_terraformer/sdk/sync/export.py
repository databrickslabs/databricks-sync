import tempfile
from pathlib import Path

from databricks_cli.sdk import ApiClient

from databricks_terraformer import log
from databricks_terraformer.sdk.config import ExportConfig
from databricks_terraformer.sdk.generators.factory import GeneratorFactory
from databricks_terraformer.sdk.git_handler import GitHandler
from databricks_terraformer.sdk.pipeline import ExportFileUtils, Pipeline


class ExportCoordinator:

    @staticmethod
    def export(api_client: ApiClient, git_url: str, yaml_file_path: Path, dry_run: bool = False,
               dask_mode: bool = False):
        client = None
        if dask_mode is True:
            from distributed import Client
            client = Client(processes=True)

        with tempfile.TemporaryDirectory() as tmp:
            base_path = Path(tmp)
            geh = GitHandler(git_url,
                             base_path,
                             delete_directory=Path(ExportFileUtils.BASE_DIRECTORY))
            config = ExportConfig.from_yaml(
                yaml_file_path)

            generator_defaults = {
                "api_client": api_client,
                "base_path": base_path
            }
            export_objects = config.objects

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
                if changes is not None:
                    log.info("Changes found.")
                    geh.commit()
                else:
                    log.info("No changes found.")
