import io
from base64 import b64decode
from typing import Text, List

from databricks_cli.dbfs.api import FileInfo, BUFFER_SIZE_BYTES
from databricks_cli.sdk import DbfsService
from databricks_cli.utils import error_and_quit


def _get_dbfs_file_data_recrusive(service: DbfsService, path):
    resp = service.list(path)
    if "files" not in resp:
        return []
    files = resp["files"]
    output = []
    for file in files:
        if file["is_dir"] is True:
            output += _get_dbfs_file_data_recrusive(service, file["path"])
        else:
            output.append(file)
    return output


def get_dbfs_files_recursive(service: DbfsService, path) -> List[DbfsService]:
    return _get_dbfs_file_data_recrusive(service, path)



