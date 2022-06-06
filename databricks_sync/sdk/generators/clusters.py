import copy
from pathlib import Path
from typing import Generator, Dict, Any, Optional

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import ClusterService, ManagedLibraryService

from databricks_sync import log
from databricks_sync.sdk.config import export_config
from databricks_sync.sdk.generators import LocalFilterBy
from databricks_sync.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.service.global_init_scripts import GlobalInitScriptsService
from databricks_sync.sdk.sync.constants import ResourceCatalog, CloudConstants, GeneratorCatalog, \
    ForEachBaseIdentifierCatalog
from databricks_sync.sdk.utils import normalize_identifier, handle_azure_libraries, \
    handle_azure_storage_info, contains_cloud_specific_storage_info, contains_cloud_specific_library_path


# TODO: document source constants in the change log
class ClusterSourceConstants:
    UI = "UI"
    JOB = "JOB"
    API = "API"

class ClusterHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None, valid_cluster_sources=None, pin_first_20=False, by=None):
        super().__init__(api_client, base_path, patterns=patterns)
        default_custom_map_vars = {"node_type_id": None,
                                   "dynamic.[*].library.content.jar": None,
                                   "dynamic.[*].library.content.whl": None,
                                   "dynamic.[*].library.content.egg": None,
                                   "driver_node_type_id": None}
        self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__service = ClusterService(self.api_client)
        self.__lib_service = ManagedLibraryService(self.api_client)
        self.__global_init_scripts_service = GlobalInitScriptsService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)
        self.__pin_first_20 = pin_first_20
        self.__max_pin_count = 20
        self.__valid_cluster_sources = valid_cluster_sources or [ClusterSourceConstants.UI, ClusterSourceConstants.API]
        self.__local_filter_by = LocalFilterBy(by, ResourceCatalog.CLUSTER_RESOURCE, self.__get_cluster_raw_id)

    def __create_cluster_data(self, cluster_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.CLUSTER_RESOURCE,
            cluster_data,
            lambda: any([self._match_patterns(cluster_data["cluster_name"])]) is False,
            self.__get_cluster_identifier,
            self.__get_cluster_raw_id,
            self.__get_cluster_dict,
            self.map_processors(self.__custom_map_vars),
            human_readable_name_func=self.__get_cluster_name,
        )

    @staticmethod
    def _handle_depends_on(tdb: TerraformDictBuilder, has_global_init_scripts):
        depends_on = []
        # If user configures dbfs files wait for that with regards to init scripts
        if export_config.contains(GeneratorCatalog.DBFS_FILE) is True:
            depends_on.append(Interpolate.depends_on(ResourceCatalog.DBFS_FILE_RESOURCE,
                                                     ForEachBaseIdentifierCatalog.DBFS_FILES_BASE_IDENTIFIER))
        # Wait for all global init scripts to be created before starting clusters
        if export_config.contains(GeneratorCatalog.GLOBAL_INIT_SCRIPT) is True and has_global_init_scripts is True:
            depends_on.append(Interpolate.depends_on(ResourceCatalog.GLOBAL_INIT_SCRIPTS_RESOURCE,
                                                     ForEachBaseIdentifierCatalog.GLOBAL_INIT_SCRIPTS_BASE_IDENTIFIER))
        if len(depends_on) > 0:
            tdb.add_optional("depends_on", lambda: depends_on)

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
                # for some reason, R library import fails when we use cran.us.r-project.org repo. We have to remove it
                if 'https://cran.us.r-project.org' in library.get('cran', {}).get('repo', ''):
                    library['cran']['repo'] = None
                resp["cloud_agnostic_libraries"] += [library]
        return resp

    async def _generate(self) -> Generator[APIData, None, None]:
        clusters = self.__service.list_clusters().get("clusters", [])
        for idx, cluster in enumerate(filter(self.__local_filter_by.is_in_criteria, clusters)):
            if "cluster_source" in cluster and cluster["cluster_source"] not in self.__valid_cluster_sources:
                continue
            cluster_spec = self.get_cluster_spec(cluster)
            if self.__pin_first_20 is True and idx < self.__max_pin_count:
                cluster_spec["is_pinned"] = True
            resp = self.get_dynamic_libraries(
                self.__lib_service.cluster_status(cluster_spec["cluster_id"]).get("library_statuses", []))

            cluster_spec["aws_libraries"] = resp["aws_libraries"]
            cluster_spec["azure_libraries"] = resp["azure_libraries"]
            cluster_spec["cloud_agnostic_libraries"] = resp["cloud_agnostic_libraries"]

            cluster_data = self.__create_cluster_data(cluster_spec)

            yield cluster_data
            try:
                yield self.__perms.create_permission_data(cluster_data, self.get_local_hcl_path,
                                                          self.get_relative_hcl_path)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.CLUSTER

    def __get_cluster_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_cluster-{d['cluster_id']}")

    @staticmethod
    def __get_cluster_raw_id(data: Dict[str, Any]) -> Optional[str]:
        return data.get('cluster_id', None)

    @staticmethod
    def __get_cluster_name(data: Dict[str, Any]) -> Optional[str]:
        return data.get('cluster_name', None)

    def __has_global_init_scripts(self) -> bool:
        # GIS not configured
        if export_config.contains(GeneratorCatalog.GLOBAL_INIT_SCRIPT) is False:
            return False
        # GIS configured but may be empty
        resp = self.__global_init_scripts_service.list_global_init_scripts()
        log.info(f"Fetched all global init scripts")
        if "scripts" not in resp:
            return False
        scripts = resp["scripts"]
        return len(scripts) > 0

    def __get_cluster_dict(self, data: Dict[str, Any]):
        return ClusterHCLGenerator.make_cluster_dict(data, depends_on=True,
                                                     has_global_init_scripts=self.__has_global_init_scripts())

    @staticmethod
    def make_cluster_dict(data: Dict[str, Any], depends_on=False, is_job=False,
                          has_global_init_scripts=False) -> Dict[str, Any]:
        id_field = "id"
        tdb = TerraformDictBuilder(ResourceCatalog.CLUSTER_RESOURCE,
                                   data, job_mode=f"{is_job}", object_id=ClusterHCLGenerator.__get_cluster_raw_id,
                                   object_name=ClusterHCLGenerator.__get_cluster_name). \
            add_required("cluster_name", lambda: data.get("cluster_name", "")). \
            add_required("spark_version", lambda: data["spark_version"]). \
            add_optional("driver_node_type_id", lambda: data["driver_node_type_id"]). \
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
            add_optional("is_pinned", lambda: data["is_pinned"]). \
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
            add_dynamic_block("azure_attributes", lambda: data["azure_attributes"], CloudConstants.AZURE). \
            add_dynamic_block("gcp_attributes", lambda: data["gcp_attributes"], CloudConstants.GCP). \
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
            ClusterHCLGenerator._handle_depends_on(tdb, has_global_init_scripts=has_global_init_scripts)
        if is_job is True:
            tdb.add_optional("node_type_id", lambda: data["node_type_id"])
        else:
            tdb.add_required("node_type_id", lambda: data["node_type_id"])

        return tdb.to_dict()
