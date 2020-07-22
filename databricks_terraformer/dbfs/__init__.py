import io
from base64 import b64decode
from typing import Text

from databricks_cli.dbfs.api import FileInfo, BUFFER_SIZE_BYTES
from databricks_cli.sdk import DbfsService
from databricks_cli.utils import error_and_quit


def get_file_contents(dbfs_service: DbfsService, dbfs_path: Text, headers=None):
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
