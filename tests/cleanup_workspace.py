from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import WorkspaceApi
from databricks_terraformer.cluster_policies.policies_service import PolicyService
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.sdk import ApiClient


def delete_clusters(cluster_api,clusters_json):
    print("Deleting clusters")
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


def delete_pools(pool_api,instance_pools_json_list):
    print("Deleting pools")
    pool_list = pool_api.list_instance_pools()
    test_pools = []
    for pool in instance_pools_json_list:
        test_pools.append(pool["instance_pool_name"])
    if "instance_pools" in pool_list:
        for pool in pool_list["instance_pools"]:
            if pool["instance_pool_name"] in test_pools:
                pool_api.delete_instance_pool(pool["instance_pool_id"])


def delete_policies(policy_service:PolicyService, cluster_policies_json_list):
    print("Deleting policies")
    policy_list = policy_service.list_policies()
    test_policies = []
    for policy in cluster_policies_json_list:
        test_policies.append(policy["name"])
    if "policies" in policy_list:
        for policy in policy_list["policies"]:
            if policy["name"] in test_policies:
                policy_service.delete_policy(policy["policy_id"])


def remove_dbfs_file(dbfs_api):
    print("Removing DBFS files")
    try:
        file_exists = dbfs_api.get_status(DbfsPath("dbfs:/example_notebook.py"))
        dbfs_api.delete(DbfsPath("dbfs:/example_notebook.py"), False)
    except:
        pass


def remove_notebook(workspace_api):
    print("Removing notebooks")
    try:
        notebook_exists = workspace_api.list_objects("/Shared/example_notebook")
        workspace_api.delete("/Shared/example_notebook", False)
    except:
        pass