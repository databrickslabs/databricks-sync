from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import InstancePoolService

from databricks_sync.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.sync.constants import ResourceCatalog, CloudConstants, DrConstants, GeneratorCatalog


class InstancePoolHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        default_custom_map_vars = {"node_type_id": "%{GREEDYDATA:variable}",
                                   "dynamic.[*].disk_spec.content.ebs_volume_type": None,
                                   "dynamic.[*].disk_spec.content.azure_disk_volume_type": None}
        self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__service = InstancePoolService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    @staticmethod
    def handle_min_idle_instance_passive(value):
        if value == 0:
            return value
        else:
            return Interpolate.ternary(
                DrConstants.PASSIVE_MODE_VARIABLE,
                0,
                value
            )

    def __create_instance_pool_data(self, instance_pool_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.INSTANCE_POOL_RESOURCE,
            instance_pool_data,
            lambda: any([self._match_patterns(instance_pool_data["instance_pool_name"])]) is False,
            self.__get_instance_pool_identifier,
            self.__get_instance_pool_raw_id,
            self.__make_instance_pool_dict,
            self.map_processors(self.__custom_map_vars),
            human_readable_name_func=self.__get_instance_pool_name
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        instance_pools = self.__service.list_instance_pools().get("instance_pools", [])
        for instance_pool in instance_pools:
            # due to Azure limitation we have to setup enable_elastic_disk to True
            #  see https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters
            instance_pool["enable_elastic_disk"] = True

            instance_pools_data = self.__create_instance_pool_data(instance_pool)

            yield instance_pools_data
            try:
                yield self.__perms.create_permission_data(instance_pools_data, self.get_local_hcl_path,
                                                          self.get_relative_hcl_path)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.INSTANCE_POOL

    def __get_instance_pool_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_instance_pool-{d['instance_pool_id']}")

    @staticmethod
    def __get_instance_pool_raw_id(data: Dict[str, Any]) -> str:
        return data['instance_pool_id']

    @staticmethod
    def __get_instance_pool_name(data: Dict[str, Any]) -> str:
        return data.get('instance_pool_name', None)

    @staticmethod
    def __make_instance_pool_dict(data: Dict[str, Any]) -> Dict[str, Any]:

        return TerraformDictBuilder(ResourceCatalog.INSTANCE_POOL_RESOURCE,
                                    data, object_id=InstancePoolHCLGenerator.__get_instance_pool_raw_id,
                                    object_name=InstancePoolHCLGenerator.__get_instance_pool_name). \
            add_required("instance_pool_name", lambda: data["instance_pool_name"]). \
            add_required("min_idle_instances", lambda: InstancePoolHCLGenerator.handle_min_idle_instance_passive(
                data["min_idle_instances"]
            )). \
            add_required("node_type_id", lambda: data["node_type_id"]). \
            add_required("idle_instance_autotermination_minutes",
                         lambda: data["idle_instance_autotermination_minutes"]). \
            add_optional("max_capacity", lambda: data["max_capacity"]). \
            add_optional("enable_elastic_disk", lambda: data["enable_elastic_disk"]). \
            add_optional("custom_tags", lambda: data["custom_tags"]). \
            add_optional("preloaded_spark_versions", lambda: data["preloaded_spark_versions"]). \
            add_dynamic_block("aws_attributes", lambda: data["aws_attributes"], CloudConstants.AWS). \
            add_dynamic_block("azure_attributes", lambda: data["azure_attributes"], CloudConstants.AZURE). \
            add_dynamic_block("gcp_attributes", lambda: data["gcp_attributes"], CloudConstants.GCP). \
            add_dynamic_block("disk_spec", lambda: {
                "disk_size": data["disk_spec"]["disk_size"],
                "disk_count": data["disk_spec"]["disk_count"],
                "ebs_volume_type": data["disk_spec"]["disk_type"].get("ebs_volume_type", None),
                "azure_disk_volume_type": data["disk_spec"]["disk_type"].get("azure_disk_volume_type", None),
            }). \
            to_dict()
