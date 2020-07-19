import os

import click
from databricks_cli.click_types import ContextObject

from databricks_terraformer import log


def git_url_option(f):
    return click.option('--git-ssh-url', '-g', type=str, required=True,
                        help="This is the github url you wish to use to manage export and import.")(f)


def delete_option(f):
    return click.option('--delete', is_flag=True,
                        help="When fetching and pulling remote state this will delete any items that are managed "
                             "and not retrieved.")(f)


def dry_run_option(f):
    def callback(ctx, param, value):  # NOQA
        if value is True:
            log.info("===RUNNING IN DRY RUN MODE===")
        return value
    return click.option('--dry-run', is_flag=True, callback=callback,
                        help="This will only log to console the actions but not commit to git remote state.")(f)


def ssh_key_option(f):
    def callback(ctx, param, value):  # NOQA
        git_ssh_cmd = f"ssh -i {value}"
        os.environ["GIT_SSH_COMMAND"] = git_ssh_cmd

    return click.option('--ssh-key-path', '-k', required=False, default="~/.ssh/id_rsa", callback=callback,
                        expose_value=False,
                        help='CLI connection profile to use. The default value is "~/.ssh/id_rsa".')(f)
