import io
from base64 import b64decode
from pathlib import Path
from typing import Generator, List, Dict, Any, Callable, Union, Tuple, Optional

from databricks_cli.dbfs.api import FileInfo, BUFFER_SIZE_BYTES
from databricks_cli.sdk import DbfsService, ApiClient
from databricks_cli.utils import error_and_quit

from databricks_sync import log
from databricks_sync.sdk.generators import PathExclusionParser, PathInclusionParser
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_sync.sdk.message import APIData, Artifact
from databricks_sync.sdk.pipeline import DownloaderAPIGenerator
from databricks_sync.sdk.sync.constants import ResourceCatalog, ForEachBaseIdentifierCatalog, DbfsFileSchema, \
    get_members, GeneratorCatalog


class DbfsFile(Artifact):

    @staticmethod
    def __get_file_contents(dbfs_service: DbfsService, dbfs_path: str, headers=None):
        abs_path = f"dbfs:{dbfs_path}"
        json = dbfs_service.get_status(abs_path, headers=headers)
        file_info = FileInfo.from_json(json)
        if file_info.is_dir:
            error_and_quit('The dbfs file {} is a directory.'.format(repr(abs_path)))
        length = file_info.file_size
        offset = 0
        output = io.BytesIO()
        while offset < length:
            response = dbfs_service.read(abs_path, offset, BUFFER_SIZE_BYTES,
                                         headers=headers)
            bytes_read = response['bytes_read']
            data = response['data']
            offset += bytes_read
            output.write(b64decode(data))
        return output.getvalue()

    def get_content(self):
        return DbfsFile.__get_file_contents(self.service, self.remote_path)


class DbfsFileHCLGenerator(DownloaderAPIGenerator):
    DBFS_FOREACH_VAR = "databricks_dbfs_file_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, dbfs_path: Union[str, List], patterns=None,
                 custom_map_vars=None, exclude_path: Optional[Union[str, List]] = None):
        super().__init__(api_client, base_path, patterns=patterns)
        if isinstance(dbfs_path, str):
            self.__dbfs_path_patterns = [dbfs_path]
        else:
            self.__dbfs_path_patterns = dbfs_path
        self.__path_inclusion = PathInclusionParser(self.__dbfs_path_patterns,
                                                    ResourceCatalog.DBFS_FILE_RESOURCE)
        self.__dbfs_path = self.__path_inclusion.base_paths
        self.__path_exclusion = PathExclusionParser(exclude_path, ResourceCatalog.DBFS_FILE_RESOURCE)
        self.__service = DbfsService(self.api_client)
        self.__custom_map_vars = custom_map_vars

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.DBFS_FILE

    def __get_dbfs_file_data_recrusive(self, service: DbfsService, path):
        # is the base path allowed
        if self.__path_exclusion.is_path_excluded(path):
            return []
        resp = service.list(path)
        if "files" not in resp:
            return []
        files = resp["files"]
        for file in files:
            if self.__path_exclusion.is_path_excluded(file['path']):
                continue
            if file["is_dir"] is True:
                log.info(f"Export DBFS folder:{file['path']}")
                yield from self.__get_dbfs_file_data_recrusive(service, file["path"])
            elif self.__path_inclusion.is_path_included(file['path']):
                log.debug(f"Fetching data for file: {file['path']}")
                yield file

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        ret_files = []
        for file in data:
            ret_files.append(DbfsFile(remote_path=data[file]["path"],
                                      local_path=self.get_local_download_path(self.__get_dbfs_identifier(data[file])),
                                      service=self.__service))
        return ret_files

    def __get_dbfs_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_dbfs_file-{d['path']}")

    @staticmethod
    def __get_dbfs_file_raw_id(data: Dict[str, Any]) -> str:
        return data["path"]

    def __make_dbfs_file_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.DBFS_FILE_RESOURCE). \
            add_for_each(lambda: self.DBFS_FOREACH_VAR, get_members(DbfsFileSchema)). \
            to_dict()

    def __get_dbfs_file_dict(self, data: Dict[str, Any], normalize_dbfs_file_name: str) -> Dict[str, Any]:
        return {
            DbfsFileSchema.PATH: data["path"],
            DbfsFileSchema.SOURCE: f'${{pathexpand("{normalize_dbfs_file_name}")}}',
        }

    def __make_dbfs_file_data(self, dbfs_file_data: Dict[str, Any], dbfs_identifier: Callable[[Dict[str, str]], str],
                              for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        dbfs_data = self._create_data(
            ResourceCatalog.DBFS_FILE_RESOURCE,
            dbfs_file_data,
            # TODO fix this when fixing match_patterns
            lambda: any([self._match_patterns(d["path"]) for _, d in dbfs_file_data.items()]) is False,
            dbfs_identifier,
            dbfs_identifier,
            self.__make_dbfs_file_dict,
            self.map_processors(self.__custom_map_vars)
        )
        dbfs_data.upsert_local_variable(self.DBFS_FOREACH_VAR, dbfs_file_data)
        dbfs_data.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        return dbfs_data

    async def _generate(self) -> Generator[APIData, None, None]:
        service = DbfsService(self.api_client)
        # Dictionary to create one hcl json file with foreach for dbfs files
        dbfs_files = {}
        dbfs_files_id_name_pairs = []
        for p in self.__dbfs_path:
            for file in self.__get_dbfs_file_data_recrusive(service, p):
                id_ = file['path']
                dbfs_files[id_] = self.__get_dbfs_file_dict(file, self.__get_dbfs_identifier(file))
                # ID and name are same for files
                dbfs_files_id_name_pairs.append((id_, id_))

        if dbfs_files != {}:
            yield self.__make_dbfs_file_data(dbfs_files,
                                             lambda x: ForEachBaseIdentifierCatalog.DBFS_FILES_BASE_IDENTIFIER,
                                             for_each_var_id_name_pairs=dbfs_files_id_name_pairs)
