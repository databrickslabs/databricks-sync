from base64 import b64decode
from typing import List

from databricks_cli.sdk import ApiClient, WorkspaceService
from databricks_cli.workspace.api import WorkspaceFileInfo

from databricks_terraformer import log


def _get_notebooks_recrusive(service: WorkspaceService, path):
    resp = service.list(path)
    if "objects" not in resp:
        return []
    objects = resp["objects"]
    output = []
    for obj in objects:
        workspace_obj = WorkspaceFileInfo.from_json(obj)
        if workspace_obj.is_notebook is True:
            output.append(workspace_obj)
        if workspace_obj.is_dir is True:
            output += _get_notebooks_recrusive(service, workspace_obj.path)
    return output


def get_content(service: WorkspaceService, path):
    data = service.export_workspace(path, format="SOURCE")
    if "content" not in data:
        log.error(f"Unable to find content for file {path}")
        return None
    return b64decode(data["content"].encode("utf-8")).decode("utf-8")


def get_workspace_notebooks_recursive(service: WorkspaceService, path) -> List[WorkspaceFileInfo]:
    return _get_notebooks_recrusive(service, path)
