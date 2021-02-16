import json
from pathlib import Path

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.groups.api import GroupsApi
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.secrets.api import SecretApi
from databricks_cli.workspace.api import WorkspaceApi

from databricks_sync.sdk.service.cluster_policies import PolicyService


def delete_jobs(client_api: ApiClient, jobs_json):
    print("Deleting Jobs")
    jobs_api = JobsApi(client_api)
    jobs_list = jobs_api.list_jobs()
    test_jobs = []
    for job in jobs_json:
        test_jobs.append(job["name"])

    if "jobs" in jobs_list:
        for job in jobs_list["jobs"]:
            if job["settings"]["name"] in test_jobs:
                jobs_api.delete_job(job["job_id"])


def delete_clusters(client_api: ApiClient, clusters_json):
    print("Deleting clusters")

    cluster_api: ClusterApi = ClusterApi(client_api)

    cluster_list = cluster_api.list_clusters()
    test_clusters = []
    for cluster in clusters_json:
        test_clusters.append(cluster["cluster_name"])
    # add our manually created cluster
    test_clusters.append("no pool std cluster 2")
    if "clusters" in cluster_list:
        for cluster in cluster_list["clusters"]:
            if cluster["cluster_name"] in test_clusters:
                cluster_api.permanent_delete(cluster["cluster_id"])


def delete_pools(client_api: ApiClient, instance_pools_json_list):
    print("Deleting pools")
    pool_api = InstancePoolsApi(client_api)
    pool_list = pool_api.list_instance_pools()
    test_pools = []
    for pool in instance_pools_json_list:
        test_pools.append(pool["instance_pool_name"])
    if "instance_pools" in pool_list:
        for pool in pool_list["instance_pools"]:
            if pool["instance_pool_name"] in test_pools:
                pool_api.delete_instance_pool(pool["instance_pool_id"])


def delete_policies(client_api: ApiClient, cluster_policies_json_list):
    print("Deleting policies")
    policy_service = PolicyService(client_api)
    policy_list = policy_service.list_policies()
    test_policies = []
    for policy in cluster_policies_json_list:
        test_policies.append(policy["name"])
    if "policies" in policy_list:
        for policy in policy_list["policies"]:
            if policy["name"] in test_policies:
                policy_service.delete_policy(policy["policy_id"])


def delete_groups(client_api: ApiClient, groups_json):
    print("Deleting groups")
    group_api = GroupsApi(client_api)
    for grp in groups_json:
        try:
            group_api.delete(grp['name'])
        except:
            pass


def delete_dbfs_files(client_api: ApiClient, dbfs_files_json, dbfs_path):
    print("Deleting files")
    dbfs_api = DbfsApi(client_api)
    for file in dbfs_files_json:
        try:
            file_dbfs_path = DbfsPath(f"{dbfs_path}{file['name']}")
            file_exists = dbfs_api.get_status(file_dbfs_path)
            if file_exists:
                dbfs_api.delete(file_dbfs_path, False)
        except:
            pass


def delete_secrets(client_api: ApiClient, secrets_json):
    print("Deleting secrets")
    secret_api = SecretApi(client_api)
    for sec in secrets_json:
        try:
            scope_name = sec['name']
            secret_api.delete_scope(scope_name)
        except:
            pass


def delete_notebooks(client_api: ApiClient, notebooks_json, nb_path_in_workspace):
    print("Deleting notebooks")
    workspace_api = WorkspaceApi(client_api)
    for notebook in notebooks_json:
        try:
            notebook_path = f"{nb_path_in_workspace}{notebook['name']}"
            notebook_exists = workspace_api.list_objects(notebook_path)
            if notebook_exists:
                workspace_api.delete(notebook_path, False)  # Not recursive
        except:
            pass


def delete_all(fixture_path, src_cloud_name, dbfs_path, nb_path_in_workspace, *argv):
    src_cl_js = Path.joinpath(fixture_path, f'{src_cloud_name}_clusters.json')
    src_ip_js = Path.joinpath(fixture_path, f'{src_cloud_name}_instance_pools.json')
    src_job_js = Path.joinpath(fixture_path, f'{src_cloud_name}_jobs.json')
    cp_js = Path.joinpath(fixture_path / 'cluster_policies.json')
    nb_js = Path.joinpath(fixture_path / 'notebooks.json')
    df_js = Path.joinpath(fixture_path / 'dbfs_files.json')
    gr_js = Path.joinpath(fixture_path / 'groups.json')
    sr_js = Path.joinpath(fixture_path / 'secrets.json')

    with open(src_cl_js) as jsonfile:
        src_clusters_json = json.load(jsonfile)
    with open(src_ip_js) as jsonfile:
        src_pools_json = json.load(jsonfile)
    with open(src_job_js) as jsonfile:
        src_jobs_json = json.load(jsonfile)
    with open(cp_js) as jsonfile:
        policies_json = json.load(jsonfile)
    with open(nb_js) as jsonfile:
        notebooks_json = json.load(jsonfile)
    with open(df_js) as jsonfile:
        dbfs_files_json = json.load(jsonfile)
    with open(gr_js) as jsonfile:
        groups_json = json.load(jsonfile)
    with open(sr_js) as jsonfile:
        secrets_json = json.load(jsonfile)

    for client_api in argv:
        delete_clusters(client_api, src_clusters_json)
        delete_pools(client_api, src_pools_json)
        delete_jobs(client_api, src_jobs_json)

        delete_policies(client_api, policies_json)
        delete_notebooks(client_api, notebooks_json, nb_path_in_workspace)
        delete_dbfs_files(client_api, dbfs_files_json, dbfs_path)
        delete_groups(client_api, groups_json)
        delete_secrets(client_api, secrets_json)
