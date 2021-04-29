import json
from pathlib import Path
from typing import Generator, Dict, Any, Tuple

from databricks_cli.sdk import ApiClient

from databricks_sync.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_sync.sdk.message import APIData, HCLConvertData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.processor import MappedGrokVariableBasicAnnotationProcessor
from databricks_sync.sdk.service.cluster_policies import PolicyService
from databricks_sync.sdk.sync.constants import ResourceCatalog, GeneratorCatalog


class ClusterPolicyHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        default_custom_map_vars = {
            "node_type_id.values.[*]": None,
            "node_type_id.value": None,
            "node_type_id.defaultValue": None,
            "driver_node_type_id.values.[*]": None,
            "driver_node_type_id.value": None,
            "driver_node_type_id.defaultValue": None,
        }
        self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__service = PolicyService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    def __pre_process_custom_map_vars(self, cluster_policy_data) -> (Dict[str, Any], Tuple[str, str]):
        return MappedGrokVariableBasicAnnotationProcessor("cluster_policy_definition", self.__custom_map_vars)\
            .process_dict(json.loads(cluster_policy_data["definition"]))

    def __create_cluster_policy_data(self, cluster_policy_data: Dict[str, Any]):
        new_definition, variables = self.__pre_process_custom_map_vars(cluster_policy_data)
        cluster_policy_data["definition"] = json.dumps(new_definition)
        hcl_data = self._create_data(
            ResourceCatalog.CLUSTER_POLICY_RESOURCE,
            cluster_policy_data,
            lambda: any([self._match_patterns(cluster_policy_data["name"])]) is False,
            self.__get_cluster_policy_identifier,
            self.__get_cluster_policy_raw_id,
            self.__make_cluster_policy_dict,
            # We do not want to run processors on the transformed terraform json but on the raw definition done by the
            # pre process function. So we will pass empty for mapping
            self.map_processors({}),
            human_readable_name_func=self.__get_cluster_policy_name
        )
        for variable_tuple in variables:
            hcl_data.add_mapped_variable(variable_tuple[0], variable_tuple[1])
        return hcl_data

    def __process(self, policy):
        cluster_policy_data = self.__create_cluster_policy_data(policy)
        yield cluster_policy_data
        try:
            yield self.__perms.create_permission_data(cluster_policy_data, self.get_local_hcl_path,
                                                      self.get_relative_hcl_path)
        except NoDirectPermissionsError:
            pass

    async def _generate(self) -> Generator[APIData, None, None]:
        policies = self.__service.list_policies()
        for policy in policies.get("policies", []):
            for data in HCLConvertData.process_data(ResourceCatalog.CLUSTER_POLICY_RESOURCE,
                                                    policy, self.__process, self.__get_cluster_policy_raw_id):
                yield data

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.CLUSTER_POLICY

    def __get_cluster_policy_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_cluster_policy-{d['policy_id']}")

    @staticmethod
    def __get_cluster_policy_raw_id(data: Dict[str, Any]) -> str:
        return data['policy_id']

    @staticmethod
    def __get_cluster_policy_name(data: Dict[str, Any]) -> str:
        return data.get('name', None)

    @staticmethod
    def __make_cluster_policy_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.CLUSTER_POLICY_RESOURCE,
                                    data, object_id=ClusterPolicyHCLGenerator.__get_cluster_policy_raw_id,
                                    object_name=ClusterPolicyHCLGenerator.__get_cluster_policy_name). \
            add_required("definition", lambda: data["definition"]). \
            add_required("name", lambda: data["name"]). \
            to_dict()
