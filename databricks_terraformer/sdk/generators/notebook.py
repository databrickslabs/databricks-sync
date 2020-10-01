from base64 import b64decode
from pathlib import Path
from typing import List, Generator, Dict, Any

from databricks_cli.sdk import WorkspaceService, ApiClient
from databricks_cli.workspace.api import WorkspaceFileInfo

from databricks_terraformer import log
from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder, Expression
from databricks_terraformer.sdk.message import Artifact, APIData
from databricks_terraformer.sdk.pipeline import DownloaderAPIGenerator


class NotebookArtifact(Artifact):

    def get_content(self):
        data = self.service.export_workspace(self.remote_path, format="SOURCE")
        if "content" not in data:
            log.error(f"Unable to find content for file {self.remote_path}")
            raise FileNotFoundError(f"Unable to find content for notebook in {self.remote_path}")
        return b64decode(data["content"].encode("utf-8")).decode("utf-8")


class NotebookHCLGenerator(DownloaderAPIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, notebook_path: str, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__notebook_path = notebook_path
        self.__service = WorkspaceService(self.api_client)
        self.__custom_map_vars = custom_map_vars or []
        self.__perms = PermissionsHelper(self.api_client)

    @property
    def folder_name(self) -> str:
        return "notebook"

    @staticmethod
    def _get_notebooks_recursive(service: WorkspaceService, path: str):
        resp = service.list(path)
        log.info(f"Fetched all files & folders from path: {path}")
        if "objects" not in resp:
            return []
        objects = resp["objects"]
        for obj in objects:
            workspace_obj = WorkspaceFileInfo.from_json(obj)
            if workspace_obj.is_notebook is True:
                # we need object id for permissions so we cant use workspace file info object
                yield obj
            if workspace_obj.is_dir is True:
                yield from NotebookHCLGenerator._get_notebooks_recursive(service, workspace_obj.path)

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        return [NotebookArtifact(remote_path=data['path'],
                                 local_path=self.get_local_download_path(self.__notebook_identifier(data)),
                                 service=self.__service)]

    def __notebook_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_notebook-{d['path']}")

    @staticmethod
    def __notebook_raw_id(data: Dict[str, Any]) -> str:
        return data['object_id']

    def __make_notebook_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        tdb = TerraformDictBuilder()
        tdb. \
            add_required("content", lambda: f'filebase64("{self.__notebook_identifier(data)}")',
                         Expression()). \
            add_required("path", lambda: data["path"]). \
            add_required("overwrite", lambda: True). \
            add_required("mkdirs", lambda: True). \
            add_required("language", lambda: data['language']). \
            add_required("format", lambda: "SOURCE")
        return tdb.to_dict()

    def __create_notebook_data(self, notebook_data: Dict[str, Any]):
        return self._create_data(ResourceCatalog.NOTEBOOK_RESOURCE,
                                 notebook_data,
                                 lambda: any([self._match_patterns(notebook_data["path"])]) is False,
                                 self.__notebook_identifier,
                                 self.__notebook_raw_id,
                                 self.__make_notebook_dict,
                                 self.map_processors(self.__custom_map_vars))

    async def _generate(self) -> Generator[APIData, None, None]:
        service = WorkspaceService(self.api_client)
        for notebook in NotebookHCLGenerator._get_notebooks_recursive(service, self.__notebook_path):
            notebook_data = self.__create_notebook_data(notebook)
            yield notebook_data
            try:
                yield self.__perms.create_permission_data(notebook_data,
                                                          self.get_local_hcl_path)
            except NoDirectPermissionsError:
                pass
#
# data = {
#     "path": "test",
#     "language": "test",
#     "aws_attributes": {
#         "test": "hello",
#         "data": "hello2"
#     }
# }
# tdb = TerraformDictBuilder()
# tdb = tdb. \
#     add_required("path", lambda: data["path"]). \
#     add_required("overwrite", lambda: True). \
#     add_required("mkdirs", lambda: True). \
#     add_required("language", lambda: data['language']). \
#     add_required("format", lambda: "SOURCE").\
#     add_cloud_optional_block("aws_attributes", lambda:data["aws_attributes"], "aws").\
#     to_dict()
# print(tdb)
