import click as click
import click_log

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.apply.cli import import_cli
from databricks_terraformer.dbfs.cli import dbfs_group
from databricks_terraformer.cluster_policies.cli import cluster_policies_group
from databricks_terraformer.instance_pools.cli import instance_pools_group
from databricks_terraformer.notebooks.cli import notebook_group
from databricks_terraformer.version import print_version_callback, version


@click.group(CONTEXT_SETTINGS)
@click.option('--version', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@click_log.simple_verbosity_option(log, '--verbosity', '-v')
def cli():
    pass


cli.add_command(cluster_policies_group, name="cluster-policies")
cli.add_command(dbfs_group, name="dbfs")
cli.add_command(instance_pools_group, name="instance-pools")
cli.add_command(notebook_group, name="notebooks")
cli.add_command(import_cli, name="import")


if __name__ == "__main__":
    cli()