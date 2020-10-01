import fnmatch
import functools

import click
from click import Context
from databricks_cli.click_types import ContextObject


def match_pattern(value, pattern):
    return fnmatch.fnmatch(value, pattern)


def pattern_option(f):
    return click.option('--pattern', default="*", help="Pattern to use to identify resources via name/etc.")(f)


def provide_pattern_func(func_name="pattern_func"):
    def apply_pattern(function):
        """
        Log an action given the action name and also debug parameters.
        """
        f = pattern_option(function)

        @functools.wraps(f)
        def decorator(*args, **kwargs):
            pattern_f = functools.partial(match_pattern, pattern=kwargs["pattern"])
            kwargs.pop("pattern")
            return f(*args, **kwargs, **{func_name: pattern_f})

        return decorator

    return apply_pattern
