import json
import os

import pytest
from click.testing import CliRunner
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import WorkspaceApi
from databricks_terraformer.sdk.service.cluster_policies import PolicyService
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.groups.api import GroupsApi
from databricks_cli.secrets.api import SecretApi

from databricks_cli.configure.provider import get_config_for_profile
from databricks_cli.sdk import ApiClient
from dotenv import load_dotenv
from pathlib import Path
from databricks_terraformer.sdk.config import ExportConfig


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
def tgt_api_config(load_env):
    target_profile = os.environ.get("AZURE_TARGET_WORKSPACE")
    return get_config_for_profile(target_profile)


@pytest.fixture(scope="session")
def env(load_env):
    conf = dict()
    conf["git_repo"] = os.environ.get("GIT_REPO")
    conf["source"] = os.environ.get("AZURE_SOURCE_WORKSPACE")
    conf["target"] = os.environ.get("AZURE_TARGET_WORKSPACE")
    conf["revision"] = os.environ.get("MASTER_REVISION")
    conf["directory"] = os.environ.get("ARTIFACT_DIR")
    conf["backup_file"] = os.environ.get("BACKUP_FILE")
    conf["destroy"] = os.environ.get("DESTROY")
    if not os.path.exists(conf["backup_file"]):
        open(conf["backup_file"], 'a').close()
    return conf


@pytest.fixture(scope="session")
def cli_runner():
    return CliRunner()

@pytest.fixture(scope="session")
def it_conf():
    path = (Path(__file__).parent / 'integration_test.yaml').absolute()
    it_config = ExportConfig.from_yaml(path)
    return it_config.to_dict()


@pytest.fixture(scope="session")
def tests_path():
    path = Path(__file__).parent
    return path


@pytest.fixture(scope="session")
def src_job_api(src_api_client: ApiClient):
    return JobsApi(src_api_client)


@pytest.fixture(scope="session")
def tgt_job_api(tgt_api_client: ApiClient):
    return JobsApi(tgt_api_client)


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
def src_group_api(src_api_client: ApiClient):
    return GroupsApi(src_api_client)


@pytest.fixture(scope="session")
def tgt_group_api(tgt_api_client: ApiClient):
    return GroupsApi(tgt_api_client)


@pytest.fixture(scope="session")
def src_secret_api(src_api_client: ApiClient):
    return SecretApi(src_api_client)


@pytest.fixture(scope="session")
def tgt_secret_api(tgt_api_client: ApiClient):
    return SecretApi(tgt_api_client)


@pytest.fixture(scope="session")
def src_create_group(src_group_api, tests_path):
    fixture_path = (tests_path / 'fixtures').absolute()
    gr_js = (fixture_path / 'groups.json').absolute()

    group_list = []
    with open(gr_js) as jsonfile:
        groups_json = json.load(jsonfile)

    for grp in groups_json:
        #ToDo :add members to the group,when its APIs are available
        src_group_api.create(grp['name'])
        group_list.append(grp['name'])

    return group_list


@pytest.fixture(scope="session")
def src_group(src_group_api, tests_path):
    fixture_path = (tests_path / 'fixtures').absolute()
    gr_js = (fixture_path / 'groups.json').absolute()

    with open(gr_js) as jsonfile:
        groups_json = json.load(jsonfile)

    it_group_list = []
    for grp in groups_json:
        it_group_list.append(grp['name'])

    src_all_list = src_group_api.list_all()
    src_groups_list = []

    if "group_names" in src_all_list:
        src_groups_list = src_all_list['group_names']

    return set(it_group_list).issubset(set(src_groups_list))


@pytest.fixture(scope="session")
def src_create_secrets(src_secret_api, tests_path):
    fixture_path = (tests_path / 'fixtures').absolute()
    sr_js = (fixture_path / 'secrets.json').absolute()

    scopes_list = []
    with open(sr_js) as jsonfile:
        secrets_json = json.load(jsonfile)

    for scp in secrets_json:
        scope_name = scp['name']
        scope_principal = scp['initial_manage_principal']
        scope_secret_key = scp['key']
        scope_string_value = scp['string_value']

        src_secret_api.create_scope(scope_name, scope_principal)
        src_secret_api.put_secret(scope_name, scope_secret_key, scope_string_value, None)
        #ToDo
        #Put Secrete ACL
        scopes_list.append(scope_name)

    return scopes_list


@pytest.fixture(scope="session")
def src_secrets(src_secret_api, tests_path):
    fixture_path = (tests_path / 'fixtures').absolute()
    sr_js = (fixture_path / 'secrets.json').absolute()

    it_scopes_name_list = []
    with open(sr_js) as jsonfile:
        secrets_json = json.load(jsonfile)
    for scp in secrets_json:
        scope_name = scp['name']
        it_scopes_name_list.append(scope_name)

    src_scope_list =src_secret_api.list_scopes()['scopes']
    src_scope_name_list = []
    for scp in src_scope_list:
        src_scope_name_list.append(scp['name'])

    return set(it_scopes_name_list).issubset(set(src_scope_name_list))


@pytest.fixture(scope="session")
def src_create_cluster(src_cluster_api, tests_path):
    fixture_path = (tests_path / 'fixtures').absolute()
    cl_js = (fixture_path / 'clusters.json').absolute()

    clusters_list = []
    with open(cl_js) as jsonfile:
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
def src_create_job(src_job_api, tests_path):
    fixture_path = (tests_path / 'fixtures').absolute()
    job_js = (fixture_path / 'jobs.json').absolute()

    jobs_list = []
    with open(job_js) as jsonfile:
        jobs_json = json.load(jsonfile)

    for job in jobs_json:
        created_job_response = src_job_api.create_job(job)
        jobs_list.append(created_job_response["job_id"])

    return jobs_list


@pytest.fixture(scope="session")
def src_create_pools(src_pool_api, tests_path):
    pools_list = []
    fixture_path = (tests_path / 'fixtures').absolute()
    ip_js = (fixture_path / 'instance_pools.json').absolute()

    with open(ip_js) as jsonfile:
        instance_pools_json_list = json.load(jsonfile)
    for pool in instance_pools_json_list:
        created_pool = src_pool_api.create_instance_pool(pool)
        pools_list.append(created_pool["instance_pool_id"])

    return pools_list


@pytest.fixture(scope="session")
def src_create_policies(src_policy_service, tests_path):
    policies_list=[]

    fixture_path = (tests_path / 'fixtures').absolute()
    cp_js = (fixture_path / 'cluster_policies.json').absolute()

    with open(cp_js) as jsonfile:
        cluster_policies_json_list = json.load(jsonfile)
    for policy in cluster_policies_json_list:
        created_policy = src_policy_service.create_policy(policy["name"], policy["definition"])
        policies_list.append(created_policy["policy_id"])

    return policies_list


@pytest.fixture(scope="session")
def src_upload_dbfs_file(src_dbfs_api, tests_path, it_conf):
    fixture_path = (tests_path / 'fixtures').absolute()
    dbfs_files_json_path = (tests_path / 'fixtures' / 'dbfs_files.json').absolute()

    files_list = []
    with open(dbfs_files_json_path) as jsonfile:
        dbfs_files_json = json.load(jsonfile)

    folder_in_shard = it_conf['objects']['dbfs_file']['dbfs_path']

    for file in dbfs_files_json:
        file_path = file['path']
        print(f"Uploading {file_path}")
        # abs_path = os.path.join(fixture_path, file_path)
        abs_path = (fixture_path / file_path / file['name']).absolute()
        dbfs_path = DbfsPath(f"{folder_in_shard}/{file['name']}")  # "dbfs:/tests/"
        src_dbfs_api.put_file(abs_path, dbfs_path, True)
        files_list.append(file['name'])

    return files_list


@pytest.fixture(scope="session")
def src_dbfs_file(src_dbfs_api, tests_path, it_conf):
    fixture_path = (tests_path / 'fixtures').absolute()
    dbfs_files_json_path = (fixture_path / 'dbfs_files.json').absolute()

    with open(dbfs_files_json_path) as jsonfile:
        dbfs_files_json = json.load(jsonfile)

    folder_in_shard = it_conf['objects']['dbfs_file']['dbfs_path']

    it_files_list = []
    for file in dbfs_files_json:
        it_files_list.append(f"{folder_in_shard}{file['name']}")

    dbfs_obj_list = src_dbfs_api.list_files(DbfsPath(f"{folder_in_shard}"))
    dbfs_file_list =[]
    for fi in dbfs_obj_list:
        dbfs_file_list.append(fi.dbfs_path.absolute_path)

    return set(it_files_list).issubset(set(dbfs_file_list))


def create_group_instance_profile():
    # SCIM API is still in preview and is not reflected in the CLI
    pass


@pytest.fixture(scope="session")
def src_upload_notebooks(src_workspace_api, tests_path, it_conf):
    fixture_path = (tests_path / 'fixtures').absolute()
    nb_json_path = (fixture_path / 'notebooks.json').absolute()

    notebooks_list = []
    with open(nb_json_path) as jsonfile:
        notebooks_json = json.load(jsonfile)

    for nb in notebooks_json:
        try:
            nb_name = nb['name']
            nb_src_dir = nb['src_dir']
            nb_filename = nb['file_name']
            nb_path = os.path.join(fixture_path, nb_src_dir, nb_filename)
            nb_format = nb['format']
            nb_lan = nb['language']
            nb_destination = f"{it_conf['objects']['notebook']['notebook_path']}{nb_name}"  # /Shared/{nb_name}
            src_workspace_api.import_workspace(nb_path, nb_destination, nb_lan, nb_format, True)
            notebooks_list.append(nb_name)
        except:
            pass
    return notebooks_list


@pytest.fixture(scope="session")
def src_notebooks(src_workspace_api, tests_path, it_conf):
    nb_json_path = (tests_path / 'fixtures' / 'notebooks.json').absolute()
    nb_destination = f"{it_conf['objects']['notebook']['notebook_path']}"  # /Shared/

    with open(nb_json_path) as jsonfile:
        notebooks_json = json.load(jsonfile)

    int_test_notebooks_list = []
    for nb in notebooks_json:
        int_test_notebooks_list.append(nb['name'])

    src_objs = src_workspace_api.list_objects(nb_destination)
    src_notebooks = []
    for fi in src_objs:
        if (fi.is_notebook):
            src_notebooks.append(fi.basename)

    return set(int_test_notebooks_list).issubset(set(src_notebooks))




# def pytest_sessionstart(session,api_client:ApiClient):
#     print("start of session")
#     print(api_client.url)



# def pytest_sessionfinish(session,exitstatus):
#     print(f"\n end of session, exit status {exitstatus}")