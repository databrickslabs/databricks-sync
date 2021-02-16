import json
import os

from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.groups.api import GroupsApi
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.sdk import ApiClient
from databricks_cli.secrets.api import SecretApi
from databricks_cli.workspace.api import WorkspaceApi

from databricks_sync.sdk.service.cluster_policies import PolicyService


def src_upload_dbfs_file(client_api: ApiClient, tests_path, it_conf):
    src_dbfs_api = DbfsApi(client_api)
    fixture_path = (tests_path / 'fixtures').absolute()
    dbfs_files_json_path = (tests_path / 'fixtures' / 'dbfs_files.json').absolute()

    files_list = []
    with open(dbfs_files_json_path) as jsonfile:
        dbfs_files_json = json.load(jsonfile)

    folder_in_shard = it_conf['objects'].get('dbfs_file', {}).get('dbfs_path', '')
    print(folder_in_shard)
    if folder_in_shard is None or folder_in_shard != '':
        for file in dbfs_files_json:
            file_path = file['path']
            print(f"Uploading {file_path}")
            # abs_path = os.path.join(fixture_path, file_path)
            abs_path = (fixture_path / file_path / file['name']).absolute()
            dbfs_path = DbfsPath(f"{folder_in_shard}/{file['name']}")  # "dbfs:/tests/"
            src_dbfs_api.put_file(abs_path, dbfs_path, True)
            files_list.append(file['name'])

    return files_list


def src_create_policies(client_api: ApiClient, tests_path):
    src_policy_service = PolicyService(client_api)
    policies_list = []

    fixture_path = (tests_path / 'fixtures').absolute()
    cp_js = (fixture_path / 'cluster_policies.json').absolute()

    with open(cp_js) as jsonfile:
        cluster_policies_json_list = json.load(jsonfile)
    for policy in cluster_policies_json_list:
        created_policy = src_policy_service.create_policy(policy["name"], policy["definition"])
        policies_list.append(created_policy["policy_id"])

    return policies_list


def src_create_pools(client_api: ApiClient, tests_path, src_cloud_name):
    src_pool_api = InstancePoolsApi(client_api)
    pools_list = []
    fixture_path = (tests_path / 'fixtures').absolute()
    ip_js = (fixture_path / f'{src_cloud_name}_instance_pools.json').absolute()

    with open(ip_js) as jsonfile:
        instance_pools_json_list = json.load(jsonfile)
    for pool in instance_pools_json_list:
        created_pool = src_pool_api.create_instance_pool(pool)
        pools_list.append(created_pool["instance_pool_id"])

    return pools_list


def src_upload_notebooks(client_api: ApiClient, tests_path, it_conf):
    src_workspace_api = WorkspaceApi(client_api)
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
            print(nb_name, nb_format)
            nb_lan = nb['language']
            nb_destination = f"{it_conf['objects'].get('notebook', {}).get('notebook_path', '')}{nb_name}"  # /Shared/{nb_name}
            import_ret = src_workspace_api.import_workspace(nb_path, nb_destination, nb_lan, nb_format, True)
            print(import_ret)
            notebooks_list.append(nb_name)
        except:
            pass
    return notebooks_list


def src_create_group(client_api: ApiClient, tests_path):
    src_group_api = GroupsApi(client_api)
    fixture_path = (tests_path / 'fixtures').absolute()
    gr_js = (fixture_path / 'groups.json').absolute()

    group_list = []
    with open(gr_js) as jsonfile:
        groups_json = json.load(jsonfile)

    for grp in groups_json:
        # ToDo :add members to the group,when its APIs are available
        src_group_api.create(grp['name'])
        group_list.append(grp['name'])

    return group_list


def src_create_secrets(client_api: ApiClient, tests_path):
    src_secret_api = SecretApi(client_api)
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
        # ToDo
        # Put Secrete ACL
        scopes_list.append(scope_name)

    return scopes_list
