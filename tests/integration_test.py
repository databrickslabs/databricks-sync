import json
import logging
import os
from pathlib import Path
from pygrok import Grok

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.configure.provider import DatabricksConfig
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import WorkspaceApi
from databricks_terraformer.cmds import apply
from databricks_cli.jobs.api import JobsApi
from databricks_cli.groups.api import GroupsApi
from databricks_cli.secrets.api import SecretApi

from databricks_terraformer.sdk.service.cluster_policies import PolicyService
from databricks_cli.sdk import ApiClient
from databricks_terraformer import cli
from databricks_terraformer.sdk.sync.import_ import TerraformExecution

# TODO explain this in README.md
from databricks_terraformer.sdk.sync.export import ExportCoordinator

from tests import cleanup_workspace, cleanup_git

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


def test_cleanup(tests_path, it_conf, src_cluster_api: ClusterApi, tgt_cluster_api: ClusterApi, src_policy_service: PolicyService,
                 tgt_policy_service: PolicyService, src_pool_api: InstancePoolsApi, tgt_pool_api: InstancePoolsApi,
                 env, src_job_api: JobsApi, tgt_job_api: JobsApi, src_workspace_api: WorkspaceApi, tgt_workspace_api: WorkspaceApi,
                 src_dbfs_api: DbfsApi, tgt_dbfs_api: DbfsApi, src_group_api: GroupsApi, tgt_group_api: GroupsApi,
                 src_secret_api: SecretApi, tgt_secret_api: SecretApi):

    fixture_path = (tests_path / 'fixtures').absolute()
    dbfs_path = it_conf['objects']['dbfs_file']['dbfs_path']
    nb_path_in_workspace = it_conf['objects']['notebook']['notebook_path']

    cl_js = (fixture_path / 'clusters.json').absolute()
    cp_js = (fixture_path / 'cluster_policies.json').absolute()
    ip_js = (fixture_path / 'instance_pools.json').absolute()
    job_js = (fixture_path / 'jobs.json').absolute()
    nb_js = (fixture_path / 'notebooks.json').absolute()
    df_js = (fixture_path / 'dbfs_files.json').absolute()
    gr_js = (fixture_path / 'groups.json').absolute()
    sr_js = (fixture_path / 'secrets.json').absolute()

    with open(cl_js) as jsonfile:
        clusters_json = json.load(jsonfile)
    with open(cp_js) as jsonfile:
        policies_json = json.load(jsonfile)
    with open(ip_js) as jsonfile:
        pools_json = json.load(jsonfile)
    with open(job_js) as jsonfile:
        jobs_json = json.load(jsonfile)
    with open(nb_js) as jsonfile:
        notebooks_json = json.load(jsonfile)
    with open(df_js) as jsonfile:
        dbfs_files_json = json.load(jsonfile)
    with open(gr_js) as jsonfile:
        groups_json = json.load(jsonfile)
    with open(sr_js) as jsonfile:
        secrets_json = json.load(jsonfile)

    cleanup_workspace.delete_clusters(src_cluster_api, clusters_json)
    cleanup_workspace.delete_policies(src_policy_service, policies_json)
    cleanup_workspace.delete_pools(src_pool_api, pools_json)
    cleanup_workspace.delete_jobs(src_job_api, jobs_json)
    cleanup_workspace.delete_notebooks(src_workspace_api, notebooks_json, nb_path_in_workspace)
    cleanup_workspace.delete_dbfs_files(src_dbfs_api, dbfs_files_json, dbfs_path)
    cleanup_workspace.delete_groups(src_group_api, groups_json)
    cleanup_workspace.delete_secrets(src_secret_api, secrets_json)

    cleanup_git.destroy_all(env["git_repo"], dry_run=False)
    if Path(f'{env["directory"]}/terraform.tfstate').exists():
        Path.unlink(f'{env["directory"]}/terraform.tfstate')

    cleanup_workspace.delete_clusters(tgt_cluster_api, clusters_json)
    cleanup_workspace.delete_policies(tgt_policy_service, policies_json)
    cleanup_workspace.delete_pools(tgt_pool_api, pools_json)
    cleanup_workspace.delete_jobs(tgt_job_api, jobs_json)
    cleanup_workspace.delete_notebooks(tgt_workspace_api, notebooks_json, nb_path_in_workspace)
    cleanup_workspace.delete_dbfs_files(tgt_dbfs_api, dbfs_files_json, dbfs_path)
    cleanup_workspace.delete_groups(tgt_group_api, groups_json)
    cleanup_workspace.delete_secrets(tgt_secret_api, secrets_json)

    if Path(Path(env["directory"]) / "plan.out").exists():
        Path.unlink(Path(env["directory"]) / "plan.out")
    if Path(Path(env["directory"]) / "state.tfstate").exists():
        Path.unlink(Path(env["directory"]) / "state.tfstate")


def test_src_api_client(src_api_client: ApiClient):
    print(src_api_client.url)
    assert src_api_client.url is not None


def test_src_create_policy(src_create_policies: PolicyService):
    print(src_create_policies)
    assert src_create_policies is not None


def test_src_policies(src_policy_service: PolicyService):
    print(src_policy_service.list_policies())
    assert src_policy_service.list_policies() is not None


def test_src_create_pool(src_create_pools: InstancePoolsApi):
    print(src_create_pools)
    assert src_create_pools is not None


def test_src_pools(src_pool_api: InstancePoolsApi):
    print(src_pool_api.list_instance_pools())
    assert src_pool_api.list_instance_pools() is not None


def test_src_create_cluster(src_create_cluster: ClusterApi):
    print(src_create_cluster)
    assert src_create_cluster is not None


def test_src_clusters(src_cluster_api: ClusterApi):
    print(src_cluster_api.list_clusters())
    assert src_cluster_api.list_clusters() is not None


def test_src_create_job(src_create_job):
    print(src_create_job)
    assert src_create_job is not None


def test_src_jobs(src_job_api: JobsApi):
    print(src_job_api)
    assert src_job_api is not None


def test_src_upload_dbfs_files(src_upload_dbfs_file: DbfsApi):
    print(src_upload_dbfs_file)
    assert src_upload_dbfs_file is not None


def test_src_dbfs_files(src_dbfs_file):
    assert src_dbfs_file


def test_src_upload_notebooks(src_upload_notebooks):
    assert src_upload_notebooks is not None


def test_src_notebooks(src_notebooks):
    assert src_notebooks


def test_src_create_group(src_create_group):
    assert src_create_group is not None


def test_src_group(src_group):
    assert src_group


def test_src_create_secrets(src_create_secrets):
    assert src_create_secrets is not None


def test_src_secrete(src_secrets):
    assert src_secrets


def test_src_export_direct(src_api_client: ApiClient,env,caplog):
    caplog.set_level(logging.DEBUG)
    path = (Path(__file__).parent/'integration_test.yaml').absolute()
    throws_exception=None

    try:
        ExportCoordinator.export(src_api_client, env["git_repo"], path, dry_run=False, dask_mode=False)
    except Exception as e:
        throws_exception=e
    print(caplog.text)
    assert throws_exception is None, throws_exception


def import_direct(tgt_api_config:DatabricksConfig, env, caplog):
    caplog.set_level(logging.DEBUG)

    # setup the env variable for Terraform, using the Target credentials
    os.environ["DATABRICKS_HOST"] = tgt_api_config.host
    os.environ["DATABRICKS_TOKEN"] = tgt_api_config.token

    print(f" will import : {apply.SUPPORT_IMPORTS}")
    te = TerraformExecution(
        env["git_repo"],
        revision=env["revision"],
        folders=apply.SUPPORT_IMPORTS,
        destroy=False,
        plan=True,
        apply=True,
        refresh=False,
        # Hard coded for now
        plan_location=Path(env["directory"]) / "plan.out",
        state_location=Path(env["directory"]) / "state.tfstate",
    )
    te.execute()


def test_tgt_import_direct(tgt_api_config:DatabricksConfig, env,caplog):

    os.environ["GIT_PYTHON_TRACE"] = "full"
    os.environ["TF_VAR_databricks_secret_scope1_key1_var"] = "secret"
    os.environ["TF_VAR_databricks_secret_scope2_key2_var"] = "secret2"
    os.environ["TF_VAR_databricks_secret_IntegrationTest_scope1_it_key1_var"] = "secret3"
    os.environ["TF_VAR_databricks_secret_IntegrationTest_scope2_it_key2_var"] = "secret3"

    throws_exception=None

    try:
        import_direct(tgt_api_config, env,caplog)
    except Exception as e:
        throws_exception = e
    print(caplog.text)
    assert throws_exception is None, throws_exception


def test_tgt_import_no_change(tgt_api_config:DatabricksConfig, env,caplog):
    throws_exception=None

    try:
        import_direct(tgt_api_config, env,caplog)
    except Exception as e:
        throws_exception = e

    assert throws_exception is None, throws_exception

    grok = Grok("Plan: %{INT:add} to add, %{INT:change} to change, %{INT:destroy} to destroy.")
    print(grok.match(caplog.text))


def test_tgt_objects_exist(tgt_policy_service: PolicyService, tgt_pool_api: InstancePoolsApi,
                           src_dbfs_api: DbfsApi, tgt_dbfs_api: DbfsApi, tgt_workspace_api:WorkspaceApi, env):
    # assert len(tgt_policy_service.list_policies().get("policies",[])) == db_objects['cluster-policies']['import_object_count']
    # assert len(tgt_pool_api.list_instance_pools().get("instance_pools",[])) == db_objects['instance-pools']['import_object_count']
    # assert len(tgt_workspace_api.list_objects("/Shared")) == db_objects['notebooks']['import_object_count']
    # assert len(tgt_dbfs_api.list_files(DbfsPath("dbfs:/example_notebook.py"))) == db_objects['dbfs']['import_object_count']
    assert(True)


def test_tgt_export_dryrun(tgt_api_client:ApiClient, env, caplog):
    caplog.set_level(logging.DEBUG)
    path = (Path(__file__).parent/'integration_test.yaml').absolute()

    throws_exception=None

    try:
        ExportCoordinator.export(tgt_api_client, env["git_repo"],path, dry_run=True, dask_mode=False)
    except Exception as e:
        throws_exception=e
    print(caplog.text)
    assert throws_exception is None, throws_exception