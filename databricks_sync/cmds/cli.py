import click as click
import click_log

from databricks_sync import CONTEXT_SETTINGS, log
from databricks_sync.cmds.apply import import_cli
from databricks_sync.cmds.export import export_cli
from databricks_sync.cmds.init import init_cli
from databricks_sync.cmds.triage import triage_cli
from databricks_sync.cmds.version import print_version_callback, version


@click.group(CONTEXT_SETTINGS)
@click.option('--version', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
@click_log.simple_verbosity_option(log, '--verbosity', '-v')
def cli():
    pass


cli.add_command(export_cli, name="export")
cli.add_command(import_cli, name="import")
cli.add_command(triage_cli, name="triage")
cli.add_command(init_cli, name="init")


if __name__ == "__main__":
    cli()