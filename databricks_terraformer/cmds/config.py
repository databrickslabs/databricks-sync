import functools
import os
import uuid

import click
from click import ClickException
from databricks_cli.click_types import ContextObject
from databricks_cli.configure.config import get_profile_from_context
from databricks_cli.configure.provider import ProfileConfigProvider
from databricks_cli.sdk import ApiClient
from databricks_cli.utils import InvalidConfigurationError

from databricks_terraformer import log
from databricks_terraformer.cmds.version import version


def absolute_path_callback(ctx, param, value):  # NOQA
    if value is not None:
        return os.path.abspath(value)
    return value


def validate_git_params(git_ssh_url, local_git_path):
    inputs = [git_ssh_url, local_git_path]
    if any(inputs) is False:
        raise ClickException("--git-ssh-url flag or --local-git-path should be provided")

    if all(inputs) is True:
        raise ClickException("Only one of --git-ssh-url or --local-git-path can be provided but not both")


def git_url_option(f):
    def callback(ctx, param, value):  # NOQA
        if value is not None and value != "":
            log.info(f"===USING REMOTE GIT REPOSITORY: {value}===")
        return value

    return click.option('--git-ssh-url', '-g', type=str, default=None, callback=callback,
                        help="This is the github url you wish to use to manage export and import.")(f)


def local_git_option(f):
    def callback(ctx, param, value):  # NOQA
        if value is not None and value != "":
            log.info(f"===USING LOCAL GIT DIRECTORY: {value}===")
        return value

    return click.option('--local-git-path', '-l', type=str, default=None, callback=callback,
                        help="This is the github url you wish to use to manage export and import.")(f)


def config_path_option(f):
    return click.option('--config-path', '-c', type=click.Path(exists=True), required=True,
                        help="This is the path to the config file for the export.")(f)


def handle_additional_debug(ctx):
    log.info("Setting debug flags on.")
    context_object: ContextObject = ctx.ensure_object(ContextObject)
    if context_object.debug_mode is True:
        os.environ["TF_LOG"] = "debug"
        os.environ["GIT_PYTHON_TRACE"] = "full"


def delete_option(f):
    return click.option('--delete', is_flag=True,
                        help="When fetching and pulling remote state this will delete any items that are managed "
                             "and not retrieved.")(f)


def dask_option(f):
    return click.option('--dask', is_flag=True, default=False,
                        help="Use dask to parallelize the process.")(f)


def dry_run_option(f):
    def callback(ctx, param, value):  # NOQA
        if value is True:
            log.info("===RUNNING IN DRY RUN MODE===")
        return value

    return click.option('--dry-run', is_flag=True, callback=callback,
                        help="This will only log to console the actions but not commit to git remote state.")(f)


def tag_option(f):
    def callback(ctx, param, value):  # NOQA
        if value is True:
            log.info("===TAGGING IS ENABLED===")
        return value

    return click.option('--tag', is_flag=True, callback=callback,
                        help="This will only log to console the actions but not commit to git remote state.")(f)


def ssh_key_option(f):
    def callback(ctx, param, value):  # NOQA
        git_ssh_cmd = f"ssh -i {value}"
        os.environ["GIT_SSH_COMMAND"] = git_ssh_cmd

    return click.option('--ssh-key-path', '-k', required=False, default="~/.ssh/id_rsa", callback=callback,
                        expose_value=False,
                        help='CLI connection profile to use. The default value is "~/.ssh/id_rsa".')(f)


def inject_profile_as_env(function):
    """
    Injects the api_client keyword argument to the wrapped function.
    All callbacks wrapped by provide_api_client expect the argument ``profile`` to be passed in.
    """

    @functools.wraps(function)
    def decorator(*args, **kwargs):
        ctx = click.get_current_context()
        command_name = "-".join(ctx.command_path.split(" ")[1:])
        command_name += "-" + str(uuid.uuid1())
        profile = get_profile_from_context()
        if profile:
            # If we request a specific profile, only get credentials from tere.
            config = ProfileConfigProvider(profile).get_config()
        else:
            raise ValueError("Please provide profile field")
        if not config or not config.is_valid:
            raise InvalidConfigurationError.for_profile(profile)
        os.environ["DATABRICKS_HOST"] = config.host
        os.environ["DATABRICKS_TOKEN"] = config.token
        return function(*args, **kwargs)

    decorator.__doc__ = function.__doc__
    return decorator


def get_user_agent():
    return f'databricks-sync-{version}'

def wrap_with_user_agent(api_client_provider_func):
    def wrap_client(function):
        @api_client_provider_func
        @functools.wraps(function)
        def modify_user_agent(*args, **kwargs):
            api_client: ApiClient = kwargs["api_client"]
            api_client.default_headers.update({"user-agent": get_user_agent()})
            kwargs["api_client"] = api_client
            return function(*args, **kwargs)

        modify_user_agent.__doc__ = function.__doc__
        return modify_user_agent

    return wrap_client
