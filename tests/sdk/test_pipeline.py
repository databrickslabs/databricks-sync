from pathlib import Path
from typing import Dict, Any, Generator, Optional, List

import pytest

from databricks_terraformer.sdk.hcl import BLOCK_PREFIX
from databricks_terraformer.sdk.message import APIData, HCLConvertData
from databricks_terraformer.sdk.pipeline import APIGenerator


class MockApiClient():
    url = "mock_url"


class MockAPIGenerator(APIGenerator):

    @property
    def folder_name(self) -> str:
        return "mock_resource"

    @property
    def _annotation_dot_paths(self) -> Dict[str, List[str]]:
        return {
            BLOCK_PREFIX: ["block_path1"]
        }

    @property
    def _resource_var_dot_paths(self) -> List[str]:
        return []

    @property
    def _map_var_dot_path_dict(self) -> Optional[Dict[str, Optional[str]]]:
        return {
            "map_path1": None
        }

    async def _generate(self) -> Generator[APIData, None, None]:
        for i in range(3):
            yield {
                "id": "myid",
                "map_path1": "hello world",
                "block_path1": [
                    {
                        "nested_path": "nest123"
                    },
                    {
                        "nested_path": "nest456"
                    },
                ]
            }

    def _define_identifier(self, data: Dict[str, Any]) -> str:
        return "custom identifier"

    def get_raw_id(self, data: Dict[str, Any]) -> str:
        return data["id"]

    def make_hcl_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return {
            **data,
            **{"make_hcl_dict": "applied"}
        }


class TestAPIGenerator:

    @pytest.fixture(autouse=True)
    def mock_api_generator(self):
        return MockAPIGenerator(MockApiClient(), Path(TestAPIGenerator.__name__))

    @pytest.mark.asyncio
    async def test_generate(self, mock_api_generator):
        assert mock_api_generator.source is not None
        items: List[HCLConvertData] = []
        async for i in mock_api_generator.generate():
            i: HCLConvertData
            items.append(i)
        assert len(items) == 3
        for hcl_data in items:
            assert hcl_data.latest_version.get("make_hcl_dict", None) == "applied"

    @pytest.mark.asyncio
    async def test_get_identifier(self, mock_api_generator):
        async for i in mock_api_generator.generate():
            assert mock_api_generator.get_identifier(i) == "custom_identifier"
