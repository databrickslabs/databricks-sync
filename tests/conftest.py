import json
import os

import pytest
from click.testing import CliRunner
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import WorkspaceApi
from databricks_terraformer.cluster_policies.policies_service import PolicyService
from databricks_cli.clusters.api import ClusterApi

from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.sdk import ApiClient
from dotenv import load_dotenv

def test_dummy(): pass


@pytest.fixture(scope="session")
def load_env():
    path = os.path.dirname(__file__)
    dotenv_path = os.path.join(path, '../.env')
    load_dotenv(dotenv_path=dotenv_path)

@pytest.fixture(scope="session")
def src_api_client(load_env):
    target_profile = os.environ.get("AZURE_SOURCE_WORKSPACE")
    config = get_config_for_profile(target_profile)
    return ApiClient(host=config.host, token=config.token)

@pytest.fixture(scope="session")
def tgt_api_client(load_env):
    target_profile = os.environ.get("AZURE_TARGET_WORKSPACE")
    config = get_config_for_profile(target_profile)
    return ApiClient(host=config.host, token=config.token)

@pytest.fixture(scope="session")
def env(load_env):
    conf = dict()
    conf["git_repo"] = os.environ.get("GIT_REPO")
    conf["source"] = os.environ.get("AZURE_SOURCE_WORKSPACE")
    conf["target"] = os.environ.get("AZURE_TARGET_WORKSPACE")
    conf["revision"] = os.environ.get("MASTER_REVISION")
    conf["directory"] = os.environ.get("ARTIFACT_DIR")
    conf["backup_file"] = os.environ.get("BACKUP_FILE")
    if not os.path.exists(conf["backup_file"]):
        open(conf["backup_file"], 'a').close()
    return conf

@pytest.fixture(scope="session")
def cli_runner():
    return CliRunner()


@pytest.fixture(scope="session")
def src_cluster_api(src_api_client:ApiClient):
    return ClusterApi(src_api_client)

@pytest.fixture(scope="session")
def tgt_cluster_api(tgt_api_client:ApiClient):
    return ClusterApi(tgt_api_client)

@pytest.fixture(scope="session")
def src_pool_api(src_api_client:ApiClient):
    return InstancePoolsApi(src_api_client)

@pytest.fixture(scope="session")
def tgt_pool_api(tgt_api_client:ApiClient):
    return InstancePoolsApi(tgt_api_client)

@pytest.fixture(scope="session")
def src_policy_service(src_api_client:ApiClient):
    return PolicyService(src_api_client)

@pytest.fixture(scope="session")
def tgt_policy_service(tgt_api_client:ApiClient):
    return PolicyService(tgt_api_client)

@pytest.fixture(scope="session")
def src_dbfs_api(src_api_client:ApiClient):
    return DbfsApi(src_api_client)

@pytest.fixture(scope="session")
def tgt_dbfs_api(tgt_api_client:ApiClient):
    return DbfsApi(tgt_api_client)

@pytest.fixture(scope="session")
def src_workspace_api(src_api_client:ApiClient):
    return WorkspaceApi(src_api_client)

@pytest.fixture(scope="session")
def tgt_workspace_api(tgt_api_client:ApiClient):
    return WorkspaceApi(tgt_api_client)

@pytest.fixture(scope="session")
def src_create_cluster(src_cluster_api):

    fixture_path = os.path.join(pytest.__pytestPDB._config.rootdir,'tests/fixtures')
    clusters_list = []
    with open(os.path.join(fixture_path,"clusters.json")) as jsonfile:
        clusters_json = json.load(jsonfile)

    for cluster in clusters_json:
        created_cluster = src_cluster_api.create_cluster(cluster)
        src_cluster_api.delete_cluster(created_cluster["cluster_id"])
        clusters_list.append(created_cluster["cluster_id"])

    # as we "manually" create a new cluster, we need to update the cleanup list as well
    clusters_json[0]["autotermination_minutes"] = 60
    clusters_json[0]["cluster_name"] = "no pool std cluster 2"
    cluster = src_cluster_api.create_cluster(clusters_json[0])
    src_cluster_api.delete_cluster(cluster["cluster_id"])
    clusters_list.append(cluster["cluster_id"])
    return clusters_list

@pytest.fixture(scope="session")
def src_create_pools(src_pool_api):
    pools_list = []

    fixture_path = os.path.join(pytest.__pytestPDB._config.rootdir,'tests/fixtures')
    with open(os.path.join(fixture_path, "instance_pools.json")) as jsonfile:
        instance_pools_json_list = json.load(jsonfile)
    for pool in instance_pools_json_list:
        created_pool = src_pool_api.create_instance_pool(pool)
        pools_list.append(created_pool["instance_pool_id"])

    return pools_list

@pytest.fixture(scope="session")
def src_create_policies(src_policy_service):
    policies_list=[]

    fixture_path = os.path.join(pytest.__pytestPDB._config.rootdir,'tests/fixtures')
    with open(os.path.join(fixture_path, "cluster_policies.json")) as jsonfile:
        cluster_policies_json_list = json.load(jsonfile)
    for policy in cluster_policies_json_list:
        created_policy = src_policy_service.create_policy(policy["name"], policy["definition"])
        policies_list.append(created_policy["policy_id"])

    return policies_list

@pytest.fixture(scope="session")
def src_upload_dbfs_file(src_dbfs_api):
    fixture_path = os.path.join(pytest.__pytestPDB._config.rootdir,'tests/fixtures')
    src_dbfs_api.put_file(os.path.join(fixture_path, "example_notebook.py"), DbfsPath("dbfs:/example_notebook.py"), True)
    return "example_notebook.py"



def create_group_instance_profile():
    # SCIM API is still in preview and is not reflected in the CLI
    pass

@pytest.fixture(scope="session")
def src_upload_notebook(src_workspace_api):
    fixture_path = os.path.join(pytest.__pytestPDB._config.rootdir,'tests/fixtures')
    src_workspace_api.import_workspace(os.path.join(fixture_path, "example_notebook.py"), "/Shared/example_notebook", "PYTHON",
                                   "SOURCE", True)
    return "example_notebook.py"






# def pytest_sessionstart(session,api_client:ApiClient):
#     print("start of session")
#     print(api_client.url)



# def pytest_sessionfinish(session,exitstatus):
#     print(f"\n end of session, exit status {exitstatus}")