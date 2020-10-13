from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient

from databricks_terraformer.sdk.sync.constants import ResourceCatalog
from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.service.cluster_policies import PolicyService


class ClusterPolicyHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = PolicyService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    def __create_cluster_policy_data(self, cluster_policy_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.CLUSTER_POLICY_RESOURCE,
            cluster_policy_data,
            lambda: any([self._match_patterns(cluster_policy_data["name"])]) is False,
            self.__get_cluster_policy_identifier,
            self.__get_cluster_policy_raw_id,
            self.__make_cluster_policy_dict,
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        policies = self.__service.list_policies()
        for policy in policies.get("policies", []):
            cluster_policy_data = self.__create_cluster_policy_data(policy)
            yield cluster_policy_data
            try:
                yield self.__perms.create_permission_data(cluster_policy_data, self.get_local_hcl_path)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return "cluster_policy"

    def __get_cluster_policy_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_cluster_policy-{d['policy_id']}")

    @staticmethod
    def __get_cluster_policy_raw_id(data: Dict[str, Any]) -> str:
        return data['policy_id']

    @staticmethod
    def __make_cluster_policy_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("definition", lambda: data["definition"]). \
            add_required("name", lambda: data["name"]). \
            to_dict()
