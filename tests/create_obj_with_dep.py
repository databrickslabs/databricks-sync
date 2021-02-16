import json
import time

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.libraries.api import LibrariesApi
from databricks_cli.sdk import ApiClient

from databricks_sync.sdk.service.cluster_policies import PolicyService


def get_id(name_key, name_value, id_name, created_objects):  # policy_id, "it_cluster_with_policy",
    id_value = None
    for obj in created_objects:
        print(obj)
        if obj[name_key] == name_value:
            id_value = obj[id_name]
    print(f" {str(id_name)} of {name_value}  -> {str(id_value)}")
    return id_value


def src_create_cluster(client_api: ApiClient, tests_path, src_cloud_name):
    src_cluster_api = ClusterApi(client_api)
    src_policy_service = PolicyService(client_api)
    src_pool_api = InstancePoolsApi(client_api)
    src_cluster_lib_api = LibrariesApi(client_api)
    fixture_path = (tests_path / 'fixtures').absolute()
    cl_js = (fixture_path / f"{src_cloud_name}_clusters.json").absolute()
    lib_js = (fixture_path / 'cluster_libraries.json').absolute()

    clusters_list = []
    with open(cl_js) as jsonfile:
        clusters_json = json.load(jsonfile)
    with open(lib_js) as jsonfile:
        libs_json = json.load(jsonfile)

    for cluster in clusters_json:
        if "policy_id" in cluster:  # replace the policy name with policy id
            policy_name = cluster["policy_id"]  # name of policy object
            actual_policy_id = get_id("name", policy_name, "policy_id", src_policy_service.list_policies()["policies"])
            cluster["policy_id"] = actual_policy_id

        if "instance_pool_id" in cluster:
            instance_pool_name = cluster["instance_pool_id"]  # name of instance pool insteada of its id
            actual_intance_pool_id = get_id("instance_pool_name", instance_pool_name, "instance_pool_id",
                                            src_pool_api.list_instance_pools()["instance_pools"])
            cluster["instance_pool_id"] = actual_intance_pool_id

        created_cluster = src_cluster_api.create_cluster(cluster)

        time.sleep(5)
        src_cluster_lib_api.install_libraries(created_cluster["cluster_id"], libs_json)

        src_cluster_api.delete_cluster(created_cluster["cluster_id"])

        clusters_list.append(created_cluster["cluster_id"])
    # time.sleep(30)
    return clusters_list


def src_create_job(client_api: ApiClient, tests_path, src_cloud_name):
    src_job_api = JobsApi(client_api)
    src_cluster_api = ClusterApi(client_api)
    fixture_path = (tests_path / 'fixtures').absolute()
    job_js = (fixture_path / f'{src_cloud_name}_jobs.json').absolute()

    jobs_list = []
    with open(job_js) as jsonfile:
        jobs_json = json.load(jsonfile)

    for job in jobs_json:
        if "existing_cluster_id" in job:
            created_clusters = src_cluster_api.list_clusters()["clusters"]
            cluster_name = job['existing_cluster_id']
            actual_cluster_id = get_id("cluster_name", cluster_name, "cluster_id", created_clusters)
            job['existing_cluster_id'] = actual_cluster_id
        created_job_response = src_job_api.create_job(job)
        jobs_list.append(created_job_response["job_id"])

    return jobs_list
