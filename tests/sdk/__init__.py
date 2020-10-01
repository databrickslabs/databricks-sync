from pathlib import Path

import pytest

from databricks_terraformer.sdk.message import Artifact, APIData, HCLConvertData

raw_id = "raw_identifier"
workspace_url = "www.workspace-url.com"
hcl_resource_identifier = "hcl-id"
data = {"test": "data"}
local_save_path = Path()
resource_name = "databricks_notebook"


class MockArtifact(Artifact):

    def get_content(self):
        return "hello mocked content"


artifacts = [MockArtifact(remote_path=Path(), local_path=Path(), service=None)]


@pytest.fixture(autouse=True)
def sample_api_data_with_artifacts():
    return APIData(raw_id, workspace_url, hcl_resource_identifier, data,
                   local_save_path, artifacts=artifacts)


@pytest.fixture(autouse=True)
def sample_api_data_without_artifacts():
    return APIData(raw_id, workspace_url, hcl_resource_identifier, data,
                   local_save_path)


@pytest.fixture(autouse=True)
def hcl_convert_data_with_no_processors(sample_api_data_with_artifacts):
    return HCLConvertData(resource_name, sample_api_data_with_artifacts)
