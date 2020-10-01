import io
from base64 import b64decode
from pathlib import Path
from typing import Generator, List, Dict, Any

from databricks_cli.dbfs.api import FileInfo, BUFFER_SIZE_BYTES
from databricks_cli.sdk import DbfsService, ApiClient
from databricks_cli.utils import error_and_quit

from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder, Expression
from databricks_terraformer.sdk.message import APIData, Artifact
from databricks_terraformer.sdk.pipeline import DownloaderAPIGenerator


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
        output = io.StringIO()
        while offset < length:
            response = dbfs_service.read(abs_path, offset, BUFFER_SIZE_BYTES,
                                         headers=headers)
            bytes_read = response['bytes_read']
            data = response['data']
            offset += bytes_read
            output.write(b64decode(data).decode("utf-8"))
        return output.getvalue()

    def get_content(self):
        return DbfsFile.__get_file_contents(self.service, self.remote_path)


class DbfsFileHCLGenerator(DownloaderAPIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, dbfs_path: str, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__dbfs_path = dbfs_path
        self.__service = DbfsService(self.api_client)
        self.__custom_map_vars=custom_map_vars

    @property
    def folder_name(self) -> str:
        return "dbfs_file"

    @staticmethod
    def __get_dbfs_file_data_recrusive(service: DbfsService, path):
        resp = service.list(path)
        if "files" not in resp:
            return []
        files = resp["files"]
        for file in files:
            if file["is_dir"] is True:
                yield from DbfsFileHCLGenerator.__get_dbfs_file_data_recrusive(service, file["path"])
            else:
                yield file

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        return [DbfsFile(remote_path=data["path"],
                         local_path=self.get_local_download_path(self.__get_dbfs_identifier(data)),
                         service=self.__service)]

    def __get_dbfs_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_dbfs_file-{d['path']}")

    @staticmethod
    def __get_dbfs_file_raw_id(data: Dict[str, Any]) -> str:
        return data["path"]

    def __make_dbfs_file_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder().\
            add_required("source", lambda: f'pathexpand("{self.__get_dbfs_identifier(data)}")', Expression()).\
            add_required("content_b64_md5", lambda: f'md5(filebase64(pathexpand("{self.__get_dbfs_identifier(data)}")))',
                         Expression()).\
            add_required("path", lambda: data["path"]). \
            add_required("overwrite", lambda: True). \
            add_required("mkdirs", lambda: True). \
            add_required("validate_remote_file", lambda: True). \
            to_dict()

    def __make_dbfs_file_data(self, dbfs_file_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.DBFS_FILE_RESOURCE,
            dbfs_file_data,
            lambda: any([self._match_patterns(dbfs_file_data["path"])]) is False,
            self.__get_dbfs_identifier,
            self.__get_dbfs_file_raw_id,
            self.__make_dbfs_file_dict,
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        service = DbfsService(self.api_client)
        for dbfs_file in DbfsFileHCLGenerator.__get_dbfs_file_data_recrusive(service, self.__dbfs_path):
            dbfs_file_data = self.__make_dbfs_file_data(dbfs_file)
            yield dbfs_file_data
