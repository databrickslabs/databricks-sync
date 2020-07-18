import click as click
import click_log

from databricks_terraformer import CONTEXT_SETTINGS, log
from databricks_terraformer.policies.cli import cluster_policies_group
from databricks_terraformer.version import print_version_callback, version


@click.group(CONTEXT_SETTINGS)
@click.option('--version', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@click_log.simple_verbosity_option(log, '--verbosity', '-v')
def cli():
    pass


cli.add_command(cluster_policies_group, name="cluster-policies")


if __name__ == "__main__":
    cli()