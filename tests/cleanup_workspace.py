from databricks_cli.dbfs.api import DbfsPath

from databricks_terraformer.sdk.service.cluster_policies import PolicyService


def delete_jobs(jobs_api, jobs_json):
    print("Deleting Jobs")
    jobs_list = jobs_api.list_jobs()
    test_jobs = []
    for job in jobs_json:
        test_jobs.append(job["name"])

    if "jobs" in jobs_list:
        for job in jobs_list["jobs"]:
            if job["settings"]["name"] in test_jobs:
                jobs_api.delete_job(job["job_id"])


def delete_clusters(cluster_api, clusters_json):
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

def delete_groups(group_api, groups_json):
    print("Deleting groups")
    for grp in groups_json:
        try:
            group_api.delete(grp['name'])
            print(f"Deleted {grp['name']}")
        except:
            pass


def delete_dbfs_files(dbfs_api, dbfs_files_json, dbfs_path):
    print("Deleting files")
    for file in dbfs_files_json:
        try:
            dbfs_path = DbfsPath(f"{dbfs_path}{file['name']}")
            file_exists = dbfs_api.get_status(dbfs_path)
            if file_exists:
                dbfs_api.delete(dbfs_path, False)
        except:
            pass

def delete_dbfs_file(dbfs_api, file_name, dbfs_path):
    print("Deleting DBFS file")
    try:
        dbfs_path = DbfsPath(f"{dbfs_path}{file_name}")
        file_exists = dbfs_api.get_status(dbfs_path)
        if file_exists:
            dbfs_api.delete(dbfs_path, False)
    except:
        pass

def delete_secrets(secret_api, secrets_json):
    print("Deleting secrets")

    for sec in secrets_json:
        try:
            scope_name = sec['name']
            secret_api.delete_scope(scope_name)
            print(f"deleted {scope_name}")
        except:
            pass


def delete_notebooks(workspace_api, notebooks_json, nb_path_in_workspace):
    print("Deleting notebooks")

    for notebook in notebooks_json:
        try:
            notebook_path = f"{nb_path_in_workspace}{notebook['name']}"
            notebook_exists = workspace_api.list_objects(notebook_path)
            if notebook_exists:
                workspace_api.delete(notebook_path, False)  # Not recursive
                print(f"Deleted {notebook_path}.")
        except:
            pass


def delete_notebook(workspace_api, notebook_name, nb_path_in_workspace):
    print("Deleting notebook")
    try:
        notebook_path = f"{nb_path_in_workspace}{notebook_name}"
        notebook_exists = workspace_api.list_objects(notebook_path)
        if notebook_exists:
            workspace_api.delete(notebook_path, False) #Not recursive
    except:
        pass