from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import InstancePoolService

from databricks_terraformer.sdk.sync.constants import ResourceCatalog
from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator


class InstancePoolHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        default_custom_map_vars = {"node_type_id": "%{GREEDYDATA:variable}"}
        self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__service = InstancePoolService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    def __create_instance_pool_data(self, instance_pool_data: Dict[str, Any]):

        var_name = f"{self.__get_instance_pool_identifier(instance_pool_data)}_var"
        instance_pool_data["var"] = f"${{var.{var_name}}}"

        ret_pool = self._create_data(
            ResourceCatalog.INSTANCE_POOL_RESOURCE,
            instance_pool_data,
            lambda: any([self._match_patterns(instance_pool_data["instance_pool_name"])]) is False,
            self.__get_instance_pool_identifier,
            self.__get_instance_pool_raw_id,
            self.__make_instance_pool_dict,
            self.map_processors(self.__custom_map_vars)
        )

        ret_pool.add_resource_variable(var_name, 0)

        return ret_pool

    async def _generate(self) -> Generator[APIData, None, None]:
        instance_pools = self.__service.list_instance_pools().get("instance_pools", [])
        for instance_pool in instance_pools:
            instance_pools_data = self.__create_instance_pool_data(instance_pool)
            yield instance_pools_data
            try:
                yield self.__perms.create_permission_data(instance_pools_data, self.get_local_hcl_path)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return "instance_pool"

    def __get_instance_pool_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_instance_pool-{d['instance_pool_id']}")

    @staticmethod
    def __get_instance_pool_raw_id(data: Dict[str, Any]) -> str:
        return data['instance_pool_id']

    @staticmethod
    def __make_instance_pool_dict(data: Dict[str, Any]) -> Dict[str, Any]:

        return TerraformDictBuilder(). \
            add_required("instance_pool_name", lambda: data["instance_pool_name"]). \
            add_required("min_idle_instances", lambda: data["var"]). \
            add_required("node_type_id", lambda: data["node_type_id"]). \
            add_required("idle_instance_autotermination_minutes",
                         lambda: data["idle_instance_autotermination_minutes"]). \
            add_optional("max_capacity", lambda: data["max_capacity"]). \
            add_optional("enable_elastic_disk", lambda: data["enable_elastic_disk"]). \
            add_optional("custom_tags", lambda: data["custom_tags"]). \
            add_optional("preloaded_spark_versions", lambda: data["preloaded_spark_versions"]). \
 \
            add_cloud_optional_block("aws_attributes", lambda: data["aws_attributes"], 'AWS'). \
            add_cloud_optional_block("disk_spec", lambda: data["disk_spec"], 'AWS'). \
 \
            to_dict()
