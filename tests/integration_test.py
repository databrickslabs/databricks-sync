import logging
import logging
import os
import traceback
from pathlib import Path
from typing import List, Dict

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk import ApiClient, WorkspaceService, DbfsService
from databricks_cli.secrets.api import SecretApi
from deepdiff import DeepDiff

from databricks_terraformer.cmds import apply
from databricks_terraformer.sdk.service.cluster_policies import PolicyService
from databricks_terraformer.sdk.service.scim import ScimService
# TODO explain this in README.md
from databricks_terraformer.sdk.sync.constants import CloudConstants
# TODO explain this in README.md
from databricks_terraformer.sdk.sync.export import ExportCoordinator
from databricks_terraformer.sdk.sync.import_ import TerraformExecution
from tests import cleanup_workspace, cleanup_git
# TODO DOC README - Describe the integration Test in the READNE. Explain .env as well
# TODO DOC README - Need to document 403 scenario due to token expiration date
from tests.create_obj_no_dep import src_create_policies, src_upload_dbfs_file, src_create_pools, src_upload_notebooks, \
    src_create_group, src_create_secrets
from tests.create_obj_with_dep import src_create_cluster, src_create_job

db_objects = {'instance-pools':
                  {'args': None,
                   'export_object_count': 2,
                   'import_object_count': 2,
                   'export_pattern': 'Writing instance_pools to path',
                   'import_pattern': r'\+ resource "databricks_instance_pool"'
                   },
              'cluster-policies':
                  {'args': None,
                   'export_object_count': 3,
                   'import_object_count': 3,
                   'export_pattern': 'Writing cluster_policies to path',
                   'import_pattern': r'\+ resource "databricks_cluster_policy"'
                   },
              'jobs':
                  {'args': None,
                   'export_object_count': 3,
                   'import_object_count': 3,
                   'export_pattern': 'Writing jobs to path',
                   'import_pattern': r'\+ resource "databricks_job"'
                   },
              'notebooks':
                  {'args': ["--notebook-path", "/Shared"],
                   # notebooks count double, the hcl and the file
                   'export_object_count': 2,
                   'import_object_count': 1,
                   'export_pattern': 'Writing notebooks to path',
                   'import_pattern': r'\+ resource "databricks_notebook"'
                   },
              'dbfs':
                  {'args': ["--dbfs-path", "/py_code.py"],
                   # DBFS count double, the hcl and the file
                   'export_object_count': 2,
                   'import_object_count': 1,
                   'export_pattern': 'Writing dbfs to path',
                   'import_pattern': r'\+ resource "databricks_dbfs_file"'
                   },
              }

db_source_workspaces = {'aws': CloudConstants.AWS, 'azure': CloudConstants.AZURE}
db_target_workspaces = {'aws': CloudConstants.AWS, 'azure': CloudConstants.AZURE}


# @pytest.mark.run(roder=1, scope="session")
def test_cleanup(tests_path, it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient,
                 env, src_cloud_name, tgt_cloud_name):
    fixture_path = (tests_path / 'fixtures').absolute()
    dbfs_path = it_conf['objects'].get('dbfs_file', {}).get('dbfs_path', '')
    nb_path_in_workspace = it_conf['objects'].get('notebook', {}).get('notebook_path', '')

    cleanup_workspace.delete_all(fixture_path, src_cloud_name, dbfs_path, nb_path_in_workspace, src_api_client,
                                 tgt_api_client)

    cleanup_git.destroy_all(env["git_repo"], dry_run=False, branch=env["revision"])
    if Path(f'{env["directory"]}/terraform.tfstate').exists():
        Path.unlink(f'{env["directory"]}/terraform.tfstate')

    if Path(Path(env["directory"]) / "plan.out").exists():
        Path.unlink(Path(env["directory"]) / "plan.out")
    if Path(Path(env["directory"]) / "state.tfstate").exists():
        Path.unlink(Path(env["directory"]) / "state.tfstate")


def test_create_object_no_dep(src_api_client: ApiClient, tests_path, it_conf, src_cloud_name):
    src_upload_dbfs_file(src_api_client, tests_path, it_conf)
    src_create_policies(src_api_client, tests_path)
    src_create_pools(src_api_client, tests_path, src_cloud_name)
    src_upload_notebooks(src_api_client, tests_path, it_conf)
    src_create_group(src_api_client, tests_path)
    src_create_secrets(src_api_client, tests_path)


def test_create_object_with_dep(src_api_client: ApiClient, tests_path, it_conf, src_cloud_name):
    src_create_cluster(src_api_client, tests_path, src_cloud_name)
    src_create_job(src_api_client, tests_path, src_cloud_name)


def test_src_export_direct(src_api_client: ApiClient, env, caplog):
    caplog.set_level(logging.DEBUG)
    path = (Path(__file__).parent / 'integration_test.yaml').absolute()
    throws_exception = None

    try:
        ExportCoordinator.export(src_api_client, path, dask_mode=False, dry_run=False, git_ssh_url=env["git_repo"],
                                 branch=env["revision"])
    except Exception as e:
        throws_exception = e
        if throws_exception is not None:
            traceback.print_exc()
    assert throws_exception is None, throws_exception


def import_direct(tgt_api_config: DatabricksConfig, tgt_api_client: ApiClient, env, caplog, tgt_cloud_name):
    caplog.set_level(logging.DEBUG)

    # setup the env variable for Terraform, using the Target credentials
    os.environ["DATABRICKS_HOST"] = tgt_api_config.host
    os.environ["DATABRICKS_TOKEN"] = tgt_api_config.token
    os.environ["TF_VAR_CLOUD"] = tgt_cloud_name  # "AZURE"#CloudConstants.AZURE
    print(f" will import : {apply.SUPPORTED_IMPORTS}")

    te = TerraformExecution(folders=apply.SUPPORTED_IMPORTS, refresh=False, revision=env["revision"], plan=True,
                            plan_location=Path(env["directory"]) / "plan.out",
                            local_state_location=Path(env["directory"]) / "state.tfstate", apply=True, destroy=False,
                            git_ssh_url=env["git_repo"], api_client=tgt_api_client, branch=env["revision"],
                            post_import_shutdown=True)
    te.execute()


def test_tgt_import_direct(tgt_api_config: DatabricksConfig, tgt_api_client: ApiClient, env, caplog, tgt_cloud_name):
    os.environ["TF_VAR_scope1_key1_var"] = "secret"
    os.environ["TF_VAR_scope2_key2_var"] = "secret2"
    os.environ["TF_VAR_IntegrationTest_scope1_it_key1_var"] = "secret3"
    os.environ["TF_VAR_IntegrationTest_scope2_it_key2_var"] = "secret3"
    os.environ["TF_VAR_uz_IntegrationTest_scope1_it_key1_var"] = "secret3"
    os.environ["TF_VAR_uz_IntegrationTest_scope2_it_key2_var"] = "secret3"

    throws_exception = None

    try:
        import_direct(tgt_api_config, tgt_api_client, env, caplog, tgt_cloud_name)
    except Exception as e:
        throws_exception = e
        if throws_exception is not None:
            traceback.print_exc()
    print(caplog.text)
    assert throws_exception is None, throws_exception


def test_cluster_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('cluster', {}) != {}:
        src_clusters = ClusterApi(src_api_client).list_clusters().get("clusters", [])
        tgt_clusters = ClusterApi(tgt_api_client).list_clusters().get("clusters", [])

        exclusion_list = ["root['termination_reason']", "root['default_tags']", "root['state_message']",
                          "root['start_time']", "root['spark_context_id']",
                          "root['cluster_id']", "root['terminated_time']", "root['policy_id']",
                          "root['instance_pool_id']",
                          "root['terminated_time']", "root['cluster_cores']", "root['driver']", "root['jdbc_port']",
                          "root['cluster_memory_mb']", "root['executors']", "root['state']"]

        # In Azure Value of root['enable_elastic_disk'] changed from False to True.
        if 'azuredatabricks.net' in src_api_client.url:
            exclusion_list.append("root['enable_elastic_disk']")

        assert compare_objects(exclusion_list, src_clusters, tgt_clusters, 'cluster_name')


def test_job_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('job', {}) != {}:
        src_jobs = JobsApi(src_api_client).list_jobs().get("jobs", [])
        tgt_jobs = JobsApi(tgt_api_client).list_jobs().get("jobs", [])

        # TODO remove ['settings']['new_cluster']['enable_elastic_disk'] once it is fixed in the provider
        exclusion_list = ["root['job_id']", "root['created_time']", "root['settings']['max_concurrent_runs']",
                          "root['settings']['existing_cluster_id']"]

        # In Azure Value of root['enable_elastic_disk'] changed from False to True.
        if 'azuredatabricks.net' in src_api_client.url:
            exclusion_list.append("root['settings']['new_cluster']['enable_elastic_disk']")

        assert compare_objects(exclusion_list, src_jobs, tgt_jobs, 'name')


def test_pool_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('instance_pool', {}) != {}:
        src_pools = InstancePoolsApi(src_api_client).list_instance_pools().get("instance_pools", [])
        tgt_pools = InstancePoolsApi(tgt_api_client).list_instance_pools().get("instance_pools", [])

        exclusion_list = ["root['default_tags']", "root['instance_pool_id']", "root['stats']"]

        # In Azure Value of root['enable_elastic_disk'] changed from False to True.
        if 'azuredatabricks.net' in src_api_client.url:
            exclusion_list.append("root['enable_elastic_disk']")

        assert compare_objects(exclusion_list, src_pools, tgt_pools, 'instance_pool_name')


def test_policy_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('cluster_policy', {}) != {}:
        src_pools = PolicyService(src_api_client).list_policies().get("policies", [])
        tgt_pools = PolicyService(tgt_api_client).list_policies().get("policies", [])
        exclusion_list = ["root['created_at_timestamp']", "root['policy_id']"]

        assert compare_objects(exclusion_list, src_pools, tgt_pools, 'name')


def test_notebook_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    notebook_path = it_conf['objects'].get('notebook', {}).get('notebook_path', '')
    if notebook_path != '':
        # @Sri - I use the Service, the Api makes it more complicated
        src_notebooks = WorkspaceService(src_api_client).list(notebook_path).get("objects", [])
        tgt_notebooks = WorkspaceService(tgt_api_client).list(notebook_path).get("objects", [])
        exclusion_list = ["root['object_id']"]

        assert compare_objects(exclusion_list, src_notebooks, tgt_notebooks, 'path')


def test_files_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    dbfs_path = it_conf['objects'].get('dbfs_file', {}).get('dbfs_path', '')
    if dbfs_path != '':
        # @Sri - I use the Service, the Api makes it more complicated
        src_files = DbfsService(src_api_client).list(dbfs_path).get("files", [])
        tgt_files = DbfsService(tgt_api_client).list(dbfs_path).get("files", [])

        exclusion_list = []

        assert compare_objects(exclusion_list, src_files, tgt_files, 'path')


def test_users_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('identity', {}) != {}:
        src_users = ScimService(src_api_client).list_users().get("Resources", [])
        tgt_users = ScimService(tgt_api_client).list_users().get("Resources", [])

        exclusion_list = ["root['groups'][0]['value']", "root['groups'][0]['$ref']"]

        assert compare_objects(exclusion_list, src_users, tgt_users, 'userName')


def test_groups_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('identity', {}) != {}:
        src_groups = ScimService(src_api_client).list_groups().get("Resources", [])
        tgt_groups = ScimService(tgt_api_client).list_groups().get("Resources", [])

        exclusion_list = ["root['id']"]

        assert compare_objects(exclusion_list, src_groups, tgt_groups, 'displayName')


def test_secrets_compare_src_tgt(it_conf, src_api_client: ApiClient, tgt_api_client: ApiClient):
    if it_conf['objects'].get('secret', {}) != {}:
        src_secret_scopes = SecretApi(src_api_client).list_scopes().get("scopes", [])
        tgt_secret_scopes = SecretApi(tgt_api_client).list_scopes().get("scopes", [])

        exclusion_list = []
        assert compare_objects(exclusion_list, src_secret_scopes, tgt_secret_scopes, 'name')

        for scope in src_secret_scopes:
            src_secret_acls = SecretApi(src_api_client).list_acls(scope['name']).get("items", [])
            tgt_secret_acls = SecretApi(tgt_api_client).list_acls(scope['name']).get("items", [])

            exclusion_list = []
            assert compare_objects(exclusion_list, src_secret_acls, tgt_secret_acls, 'principal')

        for scope in src_secret_scopes:
            src_secrets = SecretApi(src_api_client).list_secrets(scope['name']).get("secrets", [])
            tgt_secrets = SecretApi(tgt_api_client).list_secrets(scope['name']).get("secrets", [])

            exclusion_list = ["root['last_updated_timestamp']"]
            assert compare_objects(exclusion_list, src_secrets, tgt_secrets, 'key')


def compare_objects(exclusion_list: List, src_list: List, tgt_list: List, name) -> bool:
    src_dict = db_object_list_to_dict(src_list, name)
    tgt_dict = db_object_list_to_dict(tgt_list, name)

    no_diff = True
    for key in src_dict:
        ddiff = DeepDiff(src_dict[key], tgt_dict[key], exclude_paths=exclusion_list)
        if ddiff != {}:
            print(f" cluster {key} diff is:\n {ddiff.pretty()}")
            no_diff = False
    return no_diff


def db_object_list_to_dict(objects_list: List, name) -> Dict:
    objects_dict = {}
    for object in objects_list:
        # check if we're dealing with a job -> name is 'settings->'name'
        if 'settings' in object:
            objects_dict[object['settings'][name]] = object
        else:
            objects_dict[object[name]] = object
    return objects_dict
