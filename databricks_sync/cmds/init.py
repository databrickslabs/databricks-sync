from pathlib import Path

import click
from databricks_cli.configure.config import debug_option

from databricks_sync import CONTEXT_SETTINGS, log
from databricks_sync.cmds import templates

@click.command(context_settings=CONTEXT_SETTINGS, help="Initialize export configuration file.")
@click.option('--filename', '-f', required=True, help="This is the filename to create the config file for the export.")
@debug_option
@click.pass_context
def init_cli(ctx, filename):
    if Path(filename).suffix != '.yaml':
        filename = f"{filename}.yaml"
    
    with (Path.cwd() / filename).open("w+") as f:
        try:
            import importlib.resources as pkg_resources
        except ImportError:
            # Try backported to PY<37 `importlib_resources`.
            import importlib_resources as pkg_resources
        template = pkg_resources.read_text(templates, 'export.yaml')
        f.write(template)
        f.flush()
    log.info("Successfully exported configuration file %s" % (filename))