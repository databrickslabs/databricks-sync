from pathlib import Path

import click
from databricks_cli.configure.config import debug_option

from databricks_sync import CONTEXT_SETTINGS
from databricks_sync.cmds import templates


@click.command(context_settings=CONTEXT_SETTINGS, help="Initialize export configuration.")
@click.argument("filename", nargs=1)
@debug_option
@click.pass_context
def init_cli(ctx, filename):
    with (Path.cwd() / f"{filename}.yaml").open("w+") as f:
        try:
            import importlib.resources as pkg_resources
        except ImportError:
            # Try backported to PY<37 `importlib_resources`.
            import importlib_resources as pkg_resources
        template = pkg_resources.read_text(templates, 'export.yaml')
        f.write(template)
        f.flush()