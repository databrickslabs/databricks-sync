import functools
from base64 import b64decode
from pathlib import Path
from typing import List, Generator, Dict, Any, Union, Optional

from databricks_cli.sdk import WorkspaceService, ApiClient
from databricks_cli.workspace.api import WorkspaceFileInfo

from databricks_sync import log
from databricks_sync.sdk.generators import PathExclusionParser, PathInclusionParser
from databricks_sync.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import Artifact, APIData
from databricks_sync.sdk.pipeline import DownloaderAPIGenerator
from databricks_sync.sdk.service.scim import ScimService
from databricks_sync.sdk.sync.constants import ResourceCatalog, GeneratorCatalog
from databricks_sync.sdk.utils import normalize_identifier


class NotebookArtifact(Artifact):

    def get_content(self):
        data = self.service.export_workspace(self.remote_path, format="SOURCE")
        if "content" not in data:
            log.error(f"Unable to find content for file {self.remote_path}")
            raise FileNotFoundError(f"Unable to find content for notebook in {self.remote_path}")
        return b64decode(data["content"].encode("utf-8"))


class NotebookHCLGenerator(DownloaderAPIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, notebook_path: Union[str, List], patterns=None,
                 custom_map_vars=None, exclude_path: Optional[Union[str, List]] = None,
                 exclude_deleted_users: bool = False):
        super().__init__(api_client, base_path, patterns=patterns)

        if isinstance(notebook_path, str):
            self.__notebook_path_patterns = [notebook_path]
        else:
            self.__notebook_path_patterns = notebook_path
        self.__path_inclusion = PathInclusionParser(self.__notebook_path_patterns,
                                                    ResourceCatalog.NOTEBOOK_RESOURCE)
        self.__notebook_path = self.__path_inclusion.base_paths
        self.__scim_service = ScimService(self.api_client)
        self.__service = WorkspaceService(self.api_client)
        self.__custom_map_vars = custom_map_vars or {}
        self.__perms = PermissionsHelper(self.api_client)
        self.__folder_set = {}
        self.__path_exclusion = PathExclusionParser(exclude_path, ResourceCatalog.NOTEBOOK_RESOURCE)
        self.__exclude_deleted_users = exclude_deleted_users

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.NOTEBOOK

    def __get_parent_folder(self, path):
        path = Path(path)
        return str(path.parent.absolute())

    def __process_folder(self, path):
        self.__folder_set[path] = 1
        log.debug(f"Processing folder: {path} due to seeing for the first time")

    def __is_processed_folder(self, path):
        # Function should only be called once, calling it more than once will yield that path as a duplicate
        if path not in self.__folder_set:
            log.debug(f"Able to process folder: {path}")
            return False
        else:
            log.debug(f"Not processing folder: {path} due to being processed already")
            return True

    @functools.lru_cache(maxsize=None)
    def _get_valid_user_paths(self):
        users = self.__scim_service.list_users().get("Resources", [])
        return [f"/Users/{user['userName']}" for user in users]

    def _is_valid_user_path(self, path: str) -> bool:
        # Feature is disabled and move on
        if self.__exclude_deleted_users is False:
            return True
        # if its not users path move on
        if path.startswith("/Users") is False:
            return True
        # if it is just /Users or /Users/ move on
        if path == "/Users" or path == "/Users/":
            return True
        valid_user_paths = self._get_valid_user_paths()
        if any([path.startswith(user_path) for user_path in valid_user_paths]):
            return True

        return False

    def _get_notebooks_recursive(self, path: str):
        resp = self.__service.list(path)
        if self.__path_exclusion.is_path_excluded(path):
            return [], []
        if self._is_valid_user_path(path) is False:
            log.debug(f"[InvalidUserPath]: {path} is a user path for a user who is removed from the workspace.")
            return [], []

        log.info(f"Fetched all files & folders from path: {path}")
        if "objects" not in resp:
            return [], []
        objects = resp["objects"]
        first_notebook = True
        for obj in objects:
            workspace_obj = WorkspaceFileInfo.from_json(obj)
            if self.__path_exclusion.is_path_excluded(workspace_obj.path):
                continue
            if workspace_obj.is_notebook is True and self.__path_inclusion.is_path_included(workspace_obj.path):
                # we need object id for permissions so we cant use workspace file info object
                yield obj, first_notebook
                first_notebook = False
            if workspace_obj.is_dir is True:
                yield from self._get_notebooks_recursive(workspace_obj.path)

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        return [NotebookArtifact(remote_path=data['path'],
                                 local_path=self.get_local_download_path(
                                     self.__notebook_file_name(data),
                                     self.__create_custom_folder_path(data)
                                 ),
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
            return self.__create_custom_file_name(data) + extmap[lang]
        else:
            return self.__create_custom_file_name(data)

    def __notebook_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_notebook-{d['path']}"
                                                   f"-{NotebookHCLGenerator.__notebook_raw_id(data)}")

    @staticmethod
    def __notebook_raw_id(data: Dict[str, Any]) -> str:
        return data['object_id']

    @staticmethod
    def __notebook_name(data: Dict[str, Any]) -> str:
        return data.get("path", None)

    @staticmethod
    def __create_custom_hcl_path(data: Dict[str, Any]):
        folder_path = NotebookHCLGenerator.__create_custom_folder_path(data)
        if folder_path is None:
            return None
        return str(Path("hcl") / folder_path)

    @staticmethod
    def __create_custom_folder_path(data: Dict[str, Any]):
        path_s: str = data.get("path", None)
        if path_s is None:
            return None
        path: Path = Path(path_s.lstrip("/"))
        if len(path.parents) <= 1:
            return None
        return str(path.parent)

    @staticmethod
    def __create_custom_file_name(data: Dict[str, Any]):
        nbook_id = NotebookHCLGenerator.__notebook_raw_id(data)
        path_s: str = data.get("path", None)
        if path_s is None:
            return None
        path: Path = Path(path_s)
        return normalize_identifier(f"{path.name}_{nbook_id}")

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
                                 # Pattern matching not implemented using patterns field in yaml
                                 lambda: False,
                                 self.__notebook_identifier,
                                 self.__notebook_raw_id,
                                 self.__make_notebook_dict,
                                 self.map_processors(self.__custom_map_vars),
                                 human_readable_name_func=self.__notebook_name,
                                 custom_folder_path_func=self.__create_custom_hcl_path,
                                 custom_file_name_func=self.__create_custom_file_name)

    def __create_folder_data(self, dir_data: Dict[str, Any]):
        # This is a temporary stub just for permissions
        # The terraform provider does not support folders
        return self._create_data(ResourceCatalog.DIRECTORY_RESOURCE,
                                 dir_data,
                                 # Pattern matching not implemented using patterns field in yaml
                                 lambda: False,
                                 self.__notebook_identifier,
                                 self.__notebook_raw_id,
                                 self.__make_notebook_dict,
                                 self.map_processors(self.__custom_map_vars),
                                 human_readable_name_func=self.__notebook_name)

    def __folder_iter(self, notebook_obj):
        notebook_path = notebook_obj["path"]
        folder_path = notebook_path
        for parent in Path(folder_path).parents:
            if self.__is_processed_folder(parent):
                continue
            elif str(parent) == "/":
                log.debug("Cannot copy permissions for '/' folder path!")
                continue
            elif str(parent) == "/Shared":
                log.debug("Cannot modify or copy permissions for '/Shared' folder path!")
                continue
            yield parent

    def __handle_folder_permissions(self, folder_path, notebook_obj):
        # # Handle Folder permissions
        if self.__is_processed_folder(folder_path):
            return None
        elif folder_path == "/":
            log.debug("Cannot copy permissions for '/' folder path!")
            return None
        elif folder_path == "/Shared":
            log.debug("Cannot modify or copy permissions for '/Shared' folder path!")
            return None
        else:
            log.debug(f"Processing folder permissions: {folder_path}")
            folder_obj = self.__service.get_status(folder_path)
            folder_data = self.__create_folder_data(folder_obj)
            depends_on = [Interpolate.depends_on(ResourceCatalog.NOTEBOOK_RESOURCE,
                                                 self.__notebook_identifier(notebook_obj))]
            try:
                self.__process_folder(folder_path)
                return self.__perms.create_permission_data(folder_data, self.get_local_hcl_path,
                                                           self.get_relative_hcl_path, depends_on=depends_on)
            except NoDirectPermissionsError as e:
                log.debug(f"Failed folder permissions for path {folder_path} with error {str(e)}")
                return None

    async def _generate(self) -> Generator[APIData, None, None]:
        for p in self.__notebook_path:
            for notebook, first_notebook in self._get_notebooks_recursive(p):

                object_data = self.__create_notebook_data(notebook)
                yield object_data

                try:
                    yield self.__perms.create_permission_data(object_data, self.get_local_hcl_path,
                                                              self.get_relative_hcl_path)
                except NoDirectPermissionsError:
                    pass

                # Create permissions for folders
                if first_notebook is True:
                    for folder_path in self.__folder_iter(notebook):
                        folder_perms = self.__handle_folder_permissions(folder_path, notebook)
                        if folder_perms is not None:
                            yield folder_perms
