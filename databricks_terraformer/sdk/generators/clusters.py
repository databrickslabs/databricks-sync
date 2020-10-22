from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import ClusterService, ManagedLibraryService

from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder, Block
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.sync.constants import ResourceCatalog, CloudConstants
from databricks_terraformer.sdk.utils import normalize_identifier, azure_s3_dbfs


class ClusterHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        default_custom_map_vars = {"node_type_id": "%{GREEDYDATA:variable}",
                                   "driver_node_type_id": "%{GREEDYDATA:variable}"}
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
            self.__make_cluster_dict,
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        clusters = self.__service.list_clusters().get("clusters", [])
        for aws_cluster in clusters:

            azure_cluster = aws_cluster.copy()

            aws_cluster["cloud"] = CloudConstants.AWS
            azure_cluster["cloud"] = CloudConstants.AZURE
            aws_cluster["count"] = f'${{var.CLOUD == "{CloudConstants.AWS}" ? 1 : 0}}'
            azure_cluster["count"] = f'${{var.CLOUD == "{CloudConstants.AZURE}" ? 1 : 0}}'

            dbfs_block_aws = []
            dbfs_block_azure = []
            for script in aws_cluster.get("init_scripts", []):
                for key in script.keys():
                    dbfs_block_aws.append({key: script.get(key)})
                    dbfs_block_azure.append(azure_s3_dbfs(script))

            aws_cluster["init_scripts_block"] = dbfs_block_aws
            azure_cluster["init_scripts_block"] = dbfs_block_azure

            if "cluster_log_conf" in aws_cluster:
                aws_cluster["cluster_log_conf"] = aws_cluster.get("cluster_log_conf")
                azure_cluster["cluster_log_conf"] = azure_s3_dbfs(aws_cluster.get("cluster_log_conf"))

            lib_block_aws = []
            lib_block_azure = []
            for lib in self.__lib_service.cluster_status(aws_cluster["cluster_id"]).get("library_statuses", []):
                if not lib.get("is_library_for_all_clusters", ""):
                    for key in lib.get("library", {}).keys():
                        lib_block_aws.append({key: lib.get("library").get(key)})
                        lib_block_azure.append(azure_s3_dbfs(lib.get("library", {})))
            aws_cluster["libraries_block"] = lib_block_aws
            azure_cluster["libraries_block"] = lib_block_azure

            # due to Azure limitation we have to setup enable_elastic_disk to True
            #  see https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters
            azure_cluster["enable_elastic_disk"] = True

            aws_cluster_data = self.__create_cluster_data(aws_cluster)
            azure_cluster_data = self.__create_cluster_data(azure_cluster)

            yield aws_cluster_data
            yield azure_cluster_data
            try:
                yield self.__perms.create_permission_data(aws_cluster_data, self.get_local_hcl_path,
                                                          cloud_dep=CloudConstants.AWS)
                yield self.__perms.create_permission_data(azure_cluster_data, self.get_local_hcl_path,
                                                          cloud_dep=CloudConstants.AZURE)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return "cluster"

    def __get_cluster_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_cluster-{d['cluster_id']}-{d['cloud']}")

    @staticmethod
    def __get_cluster_raw_id(data: Dict[str, Any]) -> str:
        return data['cluster_id']

    @staticmethod
    def __make_cluster_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        # Normalize and interlop Instace Pool Id
        # interlop Cluster Policy Id

        return TerraformDictBuilder(). \
            add_required("cluster_name", lambda: data["cluster_name"]). \
            add_required("spark_version", lambda: data["spark_version"]). \
            add_optional("driver_node_type_id", lambda: data["driver_node_type_id"]). \
            add_required("node_type_id", lambda: data["node_type_id"]). \
            add_optional("instance_pool_id",
                         lambda: f"${{databricks_instance_pool.databricks_instance_pool{normalize_identifier(data['instance_pool_id'])}.id}}"). \
            add_optional("policy_id",
                         lambda: f"${{databricks_cluster_policy.databricks_cluster_policy_{data['policy_id']}.id}}"). \
            add_optional("autotermination_minutes", lambda: data["autotermination_minutes"]). \
            add_optional("enable_local_disk_encryption", lambda: data["enable_local_disk_encryption"]). \
            add_optional("single_user_name", lambda: data["single_user_name"]). \
            add_optional("ssh_public_keys", lambda: data["ssh_public_keys"]). \
            add_optional("spark_env_vars", lambda: data["spark_env_vars"]). \
            add_optional("custom_tags", lambda: data["custom_tags"]). \
            add_optional("spark_conf", lambda: data["spark_conf"]). \
            add_optional("autoscale", lambda: data["autoscale"]). \
 \
            add_cloud_optional_block("aws_attributes", lambda: data["aws_attributes"], CloudConstants.AWS). \
            add_optional("init_scripts", lambda: data["init_scripts_block"], Block()). \
            add_optional("library", lambda: data["libraries_block"], Block()). \
            add_optional("cluster_log_conf", lambda: data["cluster_log_conf"]). \
            add_required("count", lambda: data["count"]). \
 \
            to_dict()
