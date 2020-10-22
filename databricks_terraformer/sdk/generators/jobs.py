import copy
from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import JobsService

from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.sync.constants import ResourceCatalog, CloudConstants
from databricks_terraformer.sdk.utils import normalize_identifier, azure_s3_dbfs


class JobHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        # default_custom_map_vars = {"node_type_id":"%{GREEDYDATA:variable}"}
        # self.__custom_map_vars = {**default_custom_map_vars, **(custom_map_vars or {})}
        self.__custom_map_vars = custom_map_vars
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
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        jobs = self.__service.list_jobs().get("jobs", [])
        for aws_job in jobs:

            azure_job = copy.deepcopy(aws_job)

            aws_job["cloud"] = CloudConstants.AWS
            aws_job["count"] = f'${{var.CLOUD == "{CloudConstants.AWS}" ? 1 : 0}}'

            azure_job["cloud"] = "AZURE"
            azure_job["cloud"] = CloudConstants.AZURE
            azure_job["count"] = f'${{var.CLOUD == "{CloudConstants.AZURE}" ? 1 : 0}}'

            if "revision_timestamp" in aws_job.get("settings", []).get("notebook_task", []):
                del aws_job["settings"]["notebook_task"]["revision_timestamp"]
                del azure_job["settings"]["notebook_task"]["revision_timestamp"]

            # Existing cluster - there's no need to generate per cloud object
            # New cluster - modify the object, add var.CLOUD and generate an object per cloud
            if "new_cluster" in aws_job.get("settings", []):

                if "azure_attributes" in aws_job.get("settings", []).get("new_cluster", []):
                    del aws_job["settings"]["new_cluster"]["azure_attributes"]
                    # TODO remove this once the provider support it
                    del azure_job["settings"]["new_cluster"]["azure_attributes"]
                if "aws_attributes" in azure_job.get("settings", []).get("new_cluster", []):
                    del azure_job["settings"]["new_cluster"]["aws_attributes"]

                dbfs_block_aws = []
                dbfs_block_azure = []
                for script in aws_job.get("init_scripts", []):
                    for key in script.keys():
                        dbfs_block_aws.append({key: script.get(key)})
                        dbfs_block_azure.append(azure_s3_dbfs(script))

                aws_job["init_scripts_block"] = dbfs_block_aws
                azure_job["init_scripts_block"] = dbfs_block_azure

                if "cluster_log_conf" in aws_job:
                    azure_job["cluster_log_conf"] = azure_s3_dbfs(aws_job.get("cluster_log_conf"))

                # due to Azure limitation we have to setup enable_elastic_disk to True
                #  see https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/clusters
                azure_job["enable_elastic_disk"] = True

            else:
                aws_job['settings']['existing_cluster_id'] = \
                    f"${{databricks_cluster.databricks_cluster" \
                    f"{normalize_identifier(aws_job['settings']['existing_cluster_id'])}_{CloudConstants.AWS}[0].id}}"
                azure_job['settings']['existing_cluster_id'] = \
                    f"${{databricks_cluster.databricks_cluster" \
                    f"{normalize_identifier(azure_job['settings']['existing_cluster_id'])}_{CloudConstants.AZURE}[0].id}}"

            aws_cluster_data = self.__create_job_data(aws_job)
            azure_cluster_data = self.__create_job_data(azure_job)

            yield aws_cluster_data
            yield azure_cluster_data
            try:
                yield self.__perms.create_permission_data(aws_cluster_data, self.get_local_hcl_path,
                                                          cloud_dep=CloudConstants.AWS)
                yield self.__perms.create_permission_data(azure_cluster_data, self.get_local_hcl_path,
                                                          cloud_dep=CloudConstants.AZURE)
            except NoDirectPermissionsError:
                pass

        # TODO convert s3 to dbfs for spark_python_task

    @property
    def folder_name(self) -> str:
        return "job"

    def __get_job_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_job-{d['job_id']}-{d['cloud']}")

    @staticmethod
    def __get_job_raw_id(data: Dict[str, Any]) -> str:
        return data['job_id']

    @staticmethod
    def __make_job_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        # TODO new cluster
        # TODO library - similar to cluster?
        # TODO ommit the aws_attribute via CLOUD_FLAG
        # TODO take care for the cluster log via CLOUD_FLAG
        # TODO take care for Libraries via CLOUD_FLAG
        # TODO take care for init Script via CLOUD_FLAG
        return TerraformDictBuilder(). \
            add_optional("new_cluster", lambda: data["settings"]["new_cluster"]). \
            add_optional("name", lambda: data["settings"]["name"]). \
            add_optional("existing_cluster_id", lambda: data['settings']['existing_cluster_id']). \
            add_optional("retry_on_timeout", lambda: data["settings"]["retry_on_timeout"]). \
            add_optional("max_retries", lambda: data["settings"]["max_retries"]). \
            add_optional("timeout_seconds", lambda: data["settings"]["timeout_seconds"]). \
            add_optional("min_retry_interval_millis", lambda: data["settings"]["min_retry_interval_millis"]). \
            add_optional("max_concurrent_runs", lambda: data["settings"]["max_concurrent_runs"]). \
            add_optional("email_notifications", lambda: data["settings"]["email_notifications"]). \
            add_optional("schedule", lambda: data["settings"]["schedule"]). \
            add_optional("spark_jar_task", lambda: data["settings"]["spark_jar_task"]). \
            add_optional("spark_submit_task", lambda: data["settings"]["spark_submit_task"]). \
            add_optional("spark_python_task", lambda: data["settings"]["spark_python_task"]). \
            add_optional("notebook_task", lambda: data["settings"]["notebook_task"]). \
            add_optional("library", lambda: data["settings"]["libraries"]). \
            add_optional("count", lambda: data["count"]). \
 \
            to_dict()
