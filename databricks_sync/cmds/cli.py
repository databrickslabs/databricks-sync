import collections

import click as click
import click_log

from databricks_sync import log, CONTEXT_SETTINGS
from databricks_sync.cmds.apply import import_cli
from databricks_sync.cmds.export import export_cli
from databricks_sync.cmds.init import init_cli
from databricks_sync.cmds.triage import triage_cli
from databricks_sync.cmds.version import print_version_callback, get_version


class OrderedGroup(click.Group):
    def __init__(self, name=None, commands=None, **attrs):
        super(OrderedGroup, self).__init__(name, commands, **attrs)
        #: the registered subcommands by their exported names.
        self.commands = commands or collections.OrderedDict()

    def list_commands(self, ctx):
        return self.commands

@click.group(cls=OrderedGroup, context_settings=CONTEXT_SETTINGS)
@click.option('--version', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=get_version())
@click_log.simple_verbosity_option(log, '--verbosity', '-v')
def cli():
    pass


cli.add_command(init_cli, name="init")
cli.add_command(export_cli, name="export")
cli.add_command(import_cli, name="import")
cli.add_command(triage_cli, name="triage")


if __name__ == "__main__":
    cli()