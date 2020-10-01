from pathlib import Path

import pytest

from databricks_terraformer.sdk.config import ExportConfig
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
        config = ExportConfig.from_yaml(sample_config_file)
        assert config.name == "test"
        assert config.objects == expected_dictionary

    def test_to_dict(self, sample_config_file):
        config = ExportConfig.from_yaml(sample_config_file)
        actual = config.to_dict()
        assert {
                   "name": "test",
                   "objects": expected_dictionary
               } == actual
