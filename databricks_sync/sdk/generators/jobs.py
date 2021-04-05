from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import JobsService

from databricks_sync.sdk.generators import drop_all_but
from databricks_sync.sdk.generators.clusters import ClusterHCLGenerator
from databricks_sync.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.sync.constants import ResourceCatalog, CloudConstants, DrConstants
from databricks_sync.sdk.utils import normalize_identifier


class JobHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        # TODO: support node type id to be swapped out as a map
        default_custom_map_vars = {"new_cluster.node_type_id": None,
                                   "new_cluster.driver_node_type_id": None}
        self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__service = JobsService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    def __create_job_data(self, job_data: Dict[str, Any]):

        var_name = f"{self.__get_job_identifier(job_data)}_var"
        job_data["var"] = f"${{var.{var_name}}}"

        return self._create_data(
            ResourceCatalog.JOB_RESOURCE,
            job_data,
            lambda: any([self._match_patterns(job_data["settings"]["name"])]) is False,
            self.__get_job_identifier,
            self.__get_job_raw_id,
            self.__make_job_dict,
            self.map_processors(self.__custom_map_vars),
            human_readable_name_func=self.__get_job_name
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        jobs = self.__service.list_jobs().get("jobs", [])
        # TODO: This shouldnt be aws jobs, there is no gurantee that all jobs are aws.
        for databricks_job in jobs:
            job_data = self.__create_job_data(databricks_job)
            yield job_data
            try:
                yield self.__perms.create_permission_data(job_data, self.get_local_hcl_path, self.get_relative_hcl_path)
            except NoDirectPermissionsError:
                pass

    @property
    def folder_name(self) -> str:
        return "job"

    def __get_job_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_job-{d['job_id']}")

    @staticmethod
    def __get_job_raw_id(data: Dict[str, Any]) -> str:
        return data["job_id"]

    @staticmethod
    def __get_job_name(data: Dict[str, Any]) -> str:
        return data["settings"].get("name", None)

    @staticmethod
    def __pre_cleanse_job_data(data: Dict[str, any]) -> Dict[str, Any]:
        if "revision_timestamp" in data.get("settings", []).get("notebook_task", []):
            del data["settings"]["notebook_task"]["revision_timestamp"]
            # del azure_job["settings"]["notebook_task"]["revision_timestamp"]

        # Existing cluster - there's no need to generate per cloud object
        # New cluster - modify the object, add var.CLOUD and generate an object per cloud
        if "new_cluster" in data.get("settings", []):
            # TODO: remove this once the provider support it
            if "azure_attributes" in data.get("settings", []).get("new_cluster", []):
                del data["settings"]["new_cluster"]["azure_attributes"]

            transformed_cluster_spec = ClusterHCLGenerator. \
                get_cluster_spec(data["settings"]["new_cluster"])
            data["settings"]["new_cluster"] = \
                ClusterHCLGenerator.make_cluster_dict(transformed_cluster_spec, is_job=True)

        else:
            data['settings']['existing_cluster_id'] = Interpolate.resource(
                ResourceCatalog.CLUSTER_RESOURCE,
                f"databricks_cluster{normalize_identifier(data['settings']['existing_cluster_id'])}",
                "id",
            )
        library_resp = ClusterHCLGenerator.get_dynamic_libraries(data["settings"].get("libraries", []))
        data["aws_libraries"] = library_resp["aws_libraries"]
        data["azure_libraries"] = library_resp["azure_libraries"]
        data["cloud_agnostic_libraries"] = library_resp["cloud_agnostic_libraries"]
        return data

    @staticmethod
    def __make_job_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        name = data["settings"].get("name", "noname")
        modified_data = JobHCLGenerator.__pre_cleanse_job_data(data)
        return TerraformDictBuilder(ResourceCatalog.JOB_RESOURCE, modified_data,
                                    object_id=JobHCLGenerator.__get_job_raw_id,
                                    object_name=JobHCLGenerator.__get_job_name). \
            add_optional("new_cluster", lambda: modified_data["settings"]["new_cluster"]). \
            add_optional("name", lambda: modified_data["settings"]["name"]). \
            add_optional("existing_cluster_id", lambda: modified_data['settings']['existing_cluster_id']). \
            add_optional("retry_on_timeout", lambda: modified_data["settings"]["retry_on_timeout"]). \
            add_optional("max_retries", lambda: modified_data["settings"]["max_retries"]). \
            add_optional("timeout_seconds", lambda: modified_data["settings"]["timeout_seconds"]). \
            add_optional("min_retry_interval_millis", lambda: modified_data["settings"]["min_retry_interval_millis"]). \
            add_optional("max_concurrent_runs", lambda: modified_data["settings"]["max_concurrent_runs"]). \
            add_optional("email_notifications", lambda: drop_all_but(modified_data["settings"]["email_notifications"],
                                                                     "on_start", "on_success", "on_failure",
                                                                     "no_alert_for_skipped_runs",
                                                                     dictionary_name=f"{name}-email_notifications")). \
            add_dynamic_block("schedule", lambda: modified_data["settings"]["schedule"],
                              custom_ternary_bool_expr=f"{DrConstants.PASSIVE_MODE_VARIABLE} == false"). \
            add_optional("spark_jar_task", lambda: drop_all_but(modified_data["settings"]["spark_jar_task"],
                                                                "jar_uri", "main_class_name", "parameters",
                                                                dictionary_name=f"{name}-spark_jar_task")). \
            add_optional("spark_submit_task", lambda: modified_data["settings"]["spark_submit_task"]). \
            add_optional("spark_python_task", lambda: modified_data["settings"]["spark_python_task"]). \
            add_optional("notebook_task", lambda: modified_data["settings"]["notebook_task"]). \
            add_dynamic_blocks("library", lambda: modified_data["aws_libraries"], CloudConstants.AWS). \
            add_dynamic_blocks("library", lambda: modified_data["azure_libraries"], CloudConstants.AZURE). \
            add_dynamic_blocks("library", lambda: modified_data["cloud_agnostic_libraries"]). \
            to_dict()
