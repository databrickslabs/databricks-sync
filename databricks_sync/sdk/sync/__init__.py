from click import ClickException
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient, WorkspaceService, DbfsService
from databricks_cli.secrets.api import SecretApi

from databricks_sync import log
from databricks_sync.sdk.config import export_config
from databricks_sync.sdk.service.cluster_policies import PolicyService
from databricks_sync.sdk.service.scim import ScimService
from databricks_sync.sdk.sync.constants import DefaultDatabricksGroups, GeneratorCatalog


def validate_dict(api_client: ApiClient):
    found_exception = False
    export_objects = export_config.objects
    user_is_admin = False

    if export_objects is None:
        found_exception = True
    else:
        # validate token & admin privileges
        try:
            user_info = ScimService(api_client).me()

            for group in user_info['groups']:
                if group['display'] == DefaultDatabricksGroups.ADMIN_DATA_SOURCE_IDENTIFIER:
                    user_is_admin = True

        except Exception as e:
            found_exception = True
            log.error(f"Invalid token, please get a new one.")
            log.debug(f"Error stack:{str(e)}")

        # validate the rest of the objects
        for object_name, object_data in export_objects.items():
            if object_name == GeneratorCatalog.IDENTITY and (user_info is None or user_is_admin is False):
                log.error(
                    f"Cannot export admin objects w/o admin privileges, Remove the identity or use an admin user token")
                found_exception = True
            if object_name == GeneratorCatalog.CLUSTER and ClusterApi(api_client).list_clusters().get("clusters",
                                                                                                      []) == []:
                log.warning(f"There are no clusters to export")
            if object_name == GeneratorCatalog.CLUSTER_POLICY and PolicyService(api_client).list_policies().get(
                    "policies", []) == []:
                log.warning(f"There are no cluster policies to export")
            if object_name == GeneratorCatalog.INSTANCE_POOL and InstancePoolsApi(api_client).list_instance_pools().get(
                    "instance_pools", []) == []:
                log.warning(f"There are no pools to export")
            if object_name == GeneratorCatalog.SECRETS and SecretApi(api_client).list_scopes().get("scopes", []) == []:
                log.warning(f"There are no secrets to export")
            if object_name == GeneratorCatalog.JOB and JobsApi(api_client).list_jobs().get("jobs", []) == []:
                log.warning(f"There are no jobs to export")
            if object_name == GeneratorCatalog.NOTEBOOK:
                path = object_data['notebook_path']
                for p in path if not isinstance(path, str) else [path]:
                    try:
                        if WorkspaceService(api_client).list(p).get("objects", []) == []:
                            log.warning(f"There are no notebooks to export in {p}")
                    except Exception as e:
                        found_exception = True
                        log.error(f"At least one Notebook path ({p}) does not exists")
                        log.debug(f"Error stack:{str(e)}")
            if object_name == GeneratorCatalog.DBFS_FILE:
                path = object_data['dbfs_path']
                for p in path if not isinstance(path, str) else [path]:
                    try:
                        if DbfsService(api_client).list(p).get("files", []) == []:
                            log.warning(f"There are no files to export from {p}")
                    except Exception as e:
                        found_exception = True
                        log.error(f"At least one DBFS files path ({p}) does not exists")
                        log.debug(f"Error stack:{str(e)}")

    if found_exception is True:
        error_message = "Config file is not valid - will abort export"
        raise ClickException(error_message)
