from base64 import b64decode
from pathlib import Path
from typing import List, Generator, Dict, Any, Union

from databricks_cli.sdk import WorkspaceService, ApiClient
from databricks_cli.workspace.api import WorkspaceFileInfo

from databricks_sync import log
from databricks_sync.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import Artifact, APIData
from databricks_sync.sdk.pipeline import DownloaderAPIGenerator
from databricks_sync.sdk.sync.constants import ResourceCatalog


class NotebookArtifact(Artifact):

    def get_content(self):
        data = self.service.export_workspace(self.remote_path, format="SOURCE")
        if "content" not in data:
            log.error(f"Unable to find content for file {self.remote_path}")
            raise FileNotFoundError(f"Unable to find content for notebook in {self.remote_path}")
        return b64decode(data["content"].encode("utf-8"))


class NotebookHCLGenerator(DownloaderAPIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, notebook_path: Union[str, List], patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        if isinstance(notebook_path, str):
            self.__notebook_path = [notebook_path]
        else:
            self.__notebook_path = notebook_path
        self.__service = WorkspaceService(self.api_client)
        self.__custom_map_vars = custom_map_vars or {}
        self.__perms = PermissionsHelper(self.api_client)
        self.__folder_set = {}

    @property
    def folder_name(self) -> str:
        return "notebook"

    def __get_parent_folder(self, path):
        path = Path(path)
        return str(path.parent.absolute())

    def __is_duplicate_folder(self, path):
        # Function should only be called once, calling it more than once will yield that path as a duplicate
        if path not in self.__folder_set:
            self.__folder_set[path] = 1
            log.debug(f"Processing folder: {path} due to seeing for the first time")
            return False
        else:
            log.debug(f"Not processing folder: {path} due to being processed already")
            return True

    def _get_notebooks_recursive(self, path: str):
        resp = self.__service.list(path)
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
                yield from self._get_notebooks_recursive(workspace_obj.path)

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        return [NotebookArtifact(remote_path=data['path'],
                                 local_path=self.get_local_download_path(self.__notebook_file_name(data)),
                                 service=self.__service)]

    def __notebook_file_name(self, data: Dict[str, Any]) -> str:
        extmap = {
            "PYTHON": ".py",
            "SCALA": ".scala",
            "R": ".r",
            "SQL": ".sql"
        }
        lang = data.get("language", None)
        if lang is not None:
            return self.__notebook_identifier(data) + extmap[lang]
        else:
            return self.__notebook_identifier(data)

    def __notebook_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_notebook-{d['path']}")

    @staticmethod
    def __notebook_raw_id(data: Dict[str, Any]) -> str:
        return data['object_id']

    @staticmethod
    def __notebook_name(data: Dict[str, Any]) -> str:
        return data.get("path", None)

    def __make_notebook_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        tdb = TerraformDictBuilder(ResourceCatalog.NOTEBOOK_RESOURCE,
                                   data, object_id=NotebookHCLGenerator.__notebook_raw_id,
                                   object_name=NotebookHCLGenerator.__notebook_name)
        tdb. \
            add_required("source", lambda: self.__notebook_file_name(data)). \
            add_required("path", lambda: data["path"]). \
            add_required("language", lambda: data['language'])
        return tdb.to_dict()

    def __create_notebook_data(self, notebook_data: Dict[str, Any]):
        return self._create_data(ResourceCatalog.NOTEBOOK_RESOURCE,
                                 notebook_data,
                                 lambda: any([self._match_patterns(notebook_data["path"])]) is False,
                                 self.__notebook_identifier,
                                 self.__notebook_raw_id,
                                 self.__make_notebook_dict,
                                 self.map_processors(self.__custom_map_vars),
                                 human_readable_name_func=self.__notebook_name)

    def __create_folder_data(self, dir_data: Dict[str, Any]):
        # This is a temporary stub just for permissions
        # The terraform provider does not support folders
        return self._create_data(ResourceCatalog.DIRECTORY_RESOURCE,
                                 dir_data,
                                 lambda: any([self._match_patterns(dir_data["path"])]) is False,
                                 self.__notebook_identifier,
                                 self.__notebook_raw_id,
                                 self.__make_notebook_dict,
                                 self.map_processors(self.__custom_map_vars),
                                 human_readable_name_func=self.__notebook_name)

    def __handle_folder_permissions(self, notebook_obj):
        # Handle Folder permissions
        notebook_path = notebook_obj["path"]
        folder_path = self.__get_parent_folder(notebook_path)
        if self.__is_duplicate_folder(folder_path):
            return None
        else:
            folder_obj = self.__service.get_status(folder_path)
            folder_data = self.__create_folder_data(folder_obj)
            depends_on = [Interpolate.depends_on(ResourceCatalog.NOTEBOOK_RESOURCE,
                                                 self.__notebook_identifier(notebook_obj))]
            try:
                return self.__perms.create_permission_data(folder_data, self.get_local_hcl_path,
                                                           self.get_relative_hcl_path, depends_on=depends_on)
            except NoDirectPermissionsError:
                return None

    async def _generate(self) -> Generator[APIData, None, None]:
        for p in self.__notebook_path:
            for notebook in self._get_notebooks_recursive(p):
                # Only create hcl files for notebooks
                object_data = self.__create_notebook_data(notebook)
                yield object_data

                try:
                    yield self.__perms.create_permission_data(object_data, self.get_local_hcl_path,
                                                              self.get_relative_hcl_path)
                except NoDirectPermissionsError:
                    pass

                folder_perms = self.__handle_folder_permissions(notebook)
                if folder_perms is not None:
                    yield folder_perms
