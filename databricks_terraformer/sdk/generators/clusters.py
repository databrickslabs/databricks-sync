import copy
from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import ClusterService, ManagedLibraryService

from databricks_terraformer.sdk.config import export_config
from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.sync.constants import ResourceCatalog, CloudConstants, GeneratorCatalog, \
    ForEachBaseIdentifierCatalog
from databricks_terraformer.sdk.utils import normalize_identifier, handle_azure_libraries, \
    handle_azure_storage_info, contains_cloud_specific_storage_info, contains_cloud_specific_library_path


class ClusterHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        default_custom_map_vars = {"node_type_id": None,
                                   "dynamic.[*].library.content.jar": None,
                                   "driver_node_type_id": None}
        self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__service = ClusterService(self.api_client)
        self.__lib_service = ManagedLibraryService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    def __create_cluster_data(self, cluster_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.CLUSTER_RESOURCE,
            cluster_data,
            lambda: any([self._match_patterns(cluster_data["cluster_name"])]) is False,
            self.__get_cluster_identifier,
            self.__get_cluster_raw_id,
            self.__get_cluster_dict,
            self.map_processors(self.__custom_map_vars)
        )

    @staticmethod
    def _handle_depends_on(tdb: TerraformDictBuilder):
        if export_config.contains(GeneratorCatalog.DBFS_FILE) is True:
            depends_on_dbfs_files = [
                Interpolate.depends_on(ResourceCatalog.DBFS_FILE_RESOURCE,
                                       ForEachBaseIdentifierCatalog.DBFS_FILES_BASE_IDENTIFIER),
            ]
            tdb.add_optional("depends_on", lambda: depends_on_dbfs_files)

    @staticmethod
    def get_cluster_spec(cluster):
        data = copy.deepcopy(cluster)
        data["aws_init_scripts"] = []
        data["azure_init_scripts"] = []
        data["cloud_agnostic_init_scripts"] = []
        for script in data.get("init_scripts", []):
            if contains_cloud_specific_storage_info(script) is True:
                data["aws_init_scripts"] += [script]
                data["azure_init_scripts"] += handle_azure_storage_info(script)
            else:
                data["cloud_agnostic_init_scripts"] += [script]

        if "cluster_log_conf" in cluster:
            log_conf = cluster["cluster_log_conf"]
            if contains_cloud_specific_storage_info(log_conf) is True:
                data["aws_cluster_log_conf"] = [log_conf]
                data["azure_cluster_log_conf"] = [handle_azure_storage_info(log_conf)]
            else:
                data["cloud_agnostic_cluster_log_conf"] = [log_conf]
        return data

    @staticmethod
    def get_dynamic_libraries(libraries):
        resp = {"aws_libraries": [], "azure_libraries": [], "cloud_agnostic_libraries": []}
        for lib in libraries:
            # Skip for cluster attached libraries that are applied to all clusters
            if "is_library_for_all_clusters" in lib and lib.get("is_library_for_all_clusters") is True:
                continue
            # If using cluster status search for library otherwise then its jobs return lib
            library = lib.get("library", lib)
            if contains_cloud_specific_library_path(library) is True:
                resp["aws_libraries"] += [library]
                resp["azure_libraries"] += handle_azure_libraries(library)
            else:
                resp["cloud_agnostic_libraries"] += [library]
        return resp

    async def _generate(self) -> Generator[APIData, None, None]:
        clusters = self.__service.list_clusters().get("clusters", [])
        for cluster in clusters:
            cluster_spec = self.get_cluster_spec(cluster)

            resp = self.get_dynamic_libraries(
                self.__lib_service.cluster_status(cluster_spec["cluster_id"]).get("library_statuses", []))
            cluster_spec["aws_libraries"] = resp["aws_libraries"]
            cluster_spec["azure_libraries"] = resp["azure_libraries"]
            cluster_spec["cloud_agnostic_libraries"] = resp["cloud_agnostic_libraries"]

            cluster_data = self.__create_cluster_data(cluster_spec)

            yield cluster_data
            try:
                yield self.__perms.create_permission_data(cluster_data, self.get_local_hcl_path)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return "cluster"

    def __get_cluster_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_cluster-{d['cluster_id']}")

    @staticmethod
    def __get_cluster_raw_id(data: Dict[str, Any]) -> str:
        return data['cluster_id']

    @staticmethod
    def __get_cluster_dict(data: Dict[str, Any]):
        return ClusterHCLGenerator.make_cluster_dict(data, depends_on=True)

    @staticmethod
    def make_cluster_dict(data: Dict[str, Any], depends_on=False) -> Dict[str, Any]:
        id_field = "id"
        tdb = TerraformDictBuilder(). \
            add_required("cluster_name", lambda: data.get("cluster_name", "")). \
            add_required("spark_version", lambda: data["spark_version"]). \
            add_optional("driver_node_type_id", lambda: data["driver_node_type_id"]). \
            add_required("node_type_id", lambda: data["node_type_id"]). \
            add_optional("instance_pool_id",
                         lambda: Interpolate.resource(ResourceCatalog.INSTANCE_POOL_RESOURCE,
                                                      f"databricks_instance_pool"
                                                      f"{normalize_identifier(data['instance_pool_id'])}",
                                                      id_field)). \
            add_optional("policy_id",
                         lambda: Interpolate.resource(ResourceCatalog.CLUSTER_POLICY_RESOURCE,
                                                      f"databricks_cluster_policy_{data['policy_id']}",
                                                      id_field)). \
            add_optional("num_workers", lambda: data["num_workers"]). \
            add_optional("autotermination_minutes", lambda: data["autotermination_minutes"]). \
            add_optional("enable_local_disk_encryption", lambda: data["enable_local_disk_encryption"]). \
            add_optional("single_user_name", lambda: data["single_user_name"]). \
            add_optional("ssh_public_keys", lambda: data["ssh_public_keys"]). \
            add_optional("spark_env_vars", lambda: data["spark_env_vars"]). \
            add_optional("custom_tags", lambda: data["custom_tags"]). \
            add_optional("spark_conf", lambda: data["spark_conf"]). \
            add_optional("autoscale", lambda: data["autoscale"]). \
            add_optional("enable_elastic_disk", lambda: Interpolate.ternary(
                # due to Azure default of elastic disk we have to setup enable_elastic_disk to True
                #  see https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters
                f'{CloudConstants.CLOUD_VARIABLE} == "{CloudConstants.AZURE}"',
                "true",
                data["enable_elastic_disk"]
            )). \
            add_dynamic_block("aws_attributes", lambda: data["aws_attributes"], CloudConstants.AWS). \
            add_dynamic_blocks("init_scripts", lambda: data["aws_init_scripts"], CloudConstants.AWS). \
            add_dynamic_blocks("init_scripts", lambda: data["azure_init_scripts"], CloudConstants.AZURE). \
            add_dynamic_blocks("init_scripts", lambda: data["cloud_agnostic_init_scripts"]). \
            add_dynamic_blocks("library", lambda: data["aws_libraries"], CloudConstants.AWS). \
            add_dynamic_blocks("library", lambda: data["azure_libraries"], CloudConstants.AZURE). \
            add_dynamic_blocks("library", lambda: data["cloud_agnostic_libraries"]). \
            add_dynamic_blocks("cluster_log_conf", lambda: data["aws_cluster_log_conf"], CloudConstants.AWS). \
            add_dynamic_blocks("cluster_log_conf", lambda: data["azure_cluster_log_conf"], CloudConstants.AZURE). \
            add_dynamic_blocks("cluster_log_conf", lambda: data["cloud_agnostic_cluster_log_conf"])
        if depends_on is True:
            ClusterHCLGenerator._handle_depends_on(tdb)
        return tdb.to_dict()
