import functools
from pathlib import Path

import pytest
from databricks_cli.sdk import ApiClient

from databricks_sync.cmds.config import wrap_with_user_agent
from databricks_sync.sdk.config import ExportConfig
from tests import fixtures


@pytest.fixture
def sample_config_file():
    return Path(fixtures.__path__[0]) / "test.yaml"


expected_dictionary = {
    "notebook": {
        "notebook_path": "/demo/test/",
        "patterns": ["*"]
    },
    "cluster_policy": {
        "patterns": ["*"]
    },
    "dbfs_file": {
        "dbfs_path": "dbfs:/databricks/init_scripts"
    },
}


class TestExportConfig:
    def test_from_yaml(self, sample_config_file):
        config = ExportConfig.set_from_yaml(sample_config_file)
        assert config.name == "test"
        assert config.objects == expected_dictionary

    def test_to_dict(self, sample_config_file):
        config = ExportConfig.set_from_yaml(sample_config_file)
        actual = config.to_dict()
        assert {
                   "name": "test",
                   "objects": expected_dictionary
               } == actual


def mock_provide_api_client(function):
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        kwargs["api_client"] = ApiClient(host="localhost")
        return function(*args, **kwargs)

    return wrapper


@wrap_with_user_agent(mock_provide_api_client)
def mock_api_client_with_user_agent(api_client: ApiClient):
    return api_client


class TestCustomUserAgent:
    def test_custom_user_agent(self):
        mock_api_client = mock_api_client_with_user_agent()
        user_agent = mock_api_client.default_headers["user-agent"]
        assert "databricks-sync" in user_agent
