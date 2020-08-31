import json
import os
import pytest
import re

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.instance_pools.api import InstancePoolsApi
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.workspace.api import WorkspaceApi
from databricks_terraformer.cluster_policies.policies_service import PolicyService
from databricks_cli.sdk import ApiClient
from databricks_terraformer.cli import cli

# TODO explain this in README.md

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
              # 'jobs':
              #     {'args': None,
              #      'export_object_count': 1,
              #      'import_object_count': 1,
              #      'export_pattern': 'Writing jobs to path',
              #      'import_pattern': r'\+ resource "databricks_job"'
              #      },
              'notebooks':
                  {'args': ["--notebook-path", "/Shared"],
                   # notebooks count double, the hcl and the file
                   'export_object_count': 6,
                   'import_object_count': 3,
                   'export_pattern': 'Writing notebooks to path',
                   'import_pattern': r'\+ resource "databricks_notebook"'
                   },
              'dbfs':
                  {'args': ["--dbfs-path", "/example_notebook.py"],
                   # DBFS count double, the hcl and the file
                   'export_object_count': 2,
                   'import_object_count': 1,
                   'export_pattern': 'Writing dbfs to path',
                   'import_pattern': r'\+ resource "databricks_dbfs_file"'
                   },
              }


def test_cleanup(src_cluster_api: ClusterApi, tgt_cluster_api: ClusterApi, src_policy_service: PolicyService,
                 tgt_policy_service: PolicyService, src_pool_api: InstancePoolsApi, tgt_pool_api: InstancePoolsApi,
                 env):
    fixture_path = os.path.join(pytest.__pytestPDB._config.rootdir, 'tests/fixtures')

    with open(os.path.join(fixture_path, "clusters.json")) as jsonfile:
        clusters_json = json.load(jsonfile)
    with open(os.path.join(fixture_path, "cluster_policies.json")) as jsonfile:
        policies_json = json.load(jsonfile)
    with open(os.path.join(fixture_path, "instance_pools.json")) as jsonfile:
        pools_json = json.load(jsonfile)

    cleanup_workspace.delete_clusters(src_cluster_api, clusters_json)
    cleanup_workspace.delete_policies(src_policy_service, policies_json)
    cleanup_workspace.delete_pools(src_pool_api, pools_json)

    cleanup_git.destroy_all(env["git_repo"], dry_run=False)
    if os.path.exists(f'{env["directory"]}/terraform.tfstate'):
        os.remove(f'{env["directory"]}/terraform.tfstate')

    cleanup_workspace.delete_clusters(tgt_cluster_api, clusters_json)
    cleanup_workspace.delete_policies(tgt_policy_service, policies_json)
    cleanup_workspace.delete_pools(tgt_pool_api, pools_json)


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


def test_src_upload_dbfs_file(src_upload_dbfs_file: DbfsApi):
    print(src_upload_dbfs_file)
    assert src_upload_dbfs_file is not None


def test_src_dbfs_files(src_dbfs_api: DbfsApi):
    print(src_dbfs_api.list_files(DbfsPath("dbfs:/example_notebook.py")))
    assert src_dbfs_api.list_files(DbfsPath("dbfs:/example_notebook.py")) is not None


def test_src_upload_notebook(src_workspace_api: WorkspaceApi):
    print(src_workspace_api)
    assert src_workspace_api is not None


def test_src_notebooks(src_workspace_api: WorkspaceApi):
    print(src_workspace_api.list_objects("/Shared/example_notebook"))
    assert src_workspace_api.list_objects("/Shared/example_notebook") is not None


def test_src_export(cli_runner, env):
    global db_objects
    for run, params in db_objects.items():
        print(run)
        print(params)
        if params['args'] is None:
            result = cli_runner.invoke(cli,
                                       args=[run, 'export', '--hcl', '--profile', env["source"], '-g', env["git_repo"]],
                                       prog_name="databricks-terraformer")
        else:
            result = cli_runner.invoke(cli,
                                       args=[run, 'export', '--hcl', '--profile', env["source"], '-g', env["git_repo"],
                                             params['args'][0], params['args'][1]],
                                       prog_name="databricks-terraformer")
        if result.exit_code != 0:
            print(result.stdout)
        assert result.exit_code == 0
        assert len(re.findall(params['export_pattern'], result.stdout)) == params['export_object_count'], \
            f"export {run} found {len(re.findall(params['export_pattern'], result.stdout))} objects expected {params['export_object_count']}"


def test_tgt_import(cli_runner, env):
    global db_objects
    result = cli_runner.invoke(cli,
                               args=['import', '-g', env["git_repo"], '--profile', env["target"], "--revision",
                                     env["revision"],
                                     "--plan", "--apply", "--artifact-dir",
                                     env["directory"], "--backend-file", env["backup_file"]],
                               prog_name="databricks-terraformer")
    if result.exit_code != 0:
        print(result.stdout)
    print(result.stdout)
    assert result.exit_code == 0

    for run, params in db_objects.items():
        assert len(re.findall(params['import_pattern'], result.stdout)) == params['import_object_count'], \
            f"import {run} found {len(re.findall(params['import_pattern'], result.stdout))} objects expected {params['import_object_count']}"


def test_tgt_objects_exist(tgt_policy_service: PolicyService, tgt_pool_api: InstancePoolsApi,
                           src_dbfs_api: DbfsApi, tgt_dbfs_api: DbfsApi, tgt_workspace_api:WorkspaceApi, env):

    assert len(tgt_policy_service.list_policies()["policies"]) == db_objects['cluster-policies']['import_object_count']
    assert len(tgt_pool_api.list_instance_pools()["instance_pools"]) == db_objects['instance-pools']['import_object_count']
    assert len(tgt_workspace_api.list_objects("/Shared")) == db_objects['notebooks']['import_object_count']
    assert len(tgt_dbfs_api.list_files(DbfsPath("dbfs:/example_notebook.py"))) == db_objects['dbfs']['import_object_count']



def test_tgt_export_dryrun(cli_runner, env):
    global db_objects
    for run, params in db_objects.items():
        print(run)
        print(params)
        if params['args'] is None:
            result = cli_runner.invoke(cli,
                                       args=[run, 'export', '--hcl', '--profile', env["target"], '-g', env["git_repo"],
                                             "--dry-run"],
                                       prog_name="databricks-terraformer")
        else:
            result = cli_runner.invoke(cli,
                                       args=[run, 'export', '--hcl', '--profile', env["target"], '-g', env["git_repo"],
                                             "--dry-run",
                                             params['args'][0], params['args'][1]],
                                       prog_name="databricks-terraformer")
        if result.exit_code != 0:
            print(result.stdout)
        assert result.exit_code == 0
        assert len(re.findall(params['export_pattern'], result.stdout)) == params['export_object_count'], \
            f"export {run} found {len(re.findall(params['export_pattern'], result.stdout))} objects expected {params['export_object_count']}"
