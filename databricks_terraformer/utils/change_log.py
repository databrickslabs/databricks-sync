# Capture our current directory
import datetime
import os
from typing import Text, List

from jinja2 import Environment, FileSystemLoader


def create_change_log(resource_name: Text,
                      tag: Text,
                      date: datetime.datetime,
                      added_files: List[Text],
                      modified_files: List[Text],
                      removed_files: List[Text],
                      previous: Text,
                      base_path: Text,
                      change_log_name="README.md"
                      ):
    cur_dir = os.path.dirname(os.path.abspath(__file__))

    j2_env = Environment(loader=FileSystemLoader(cur_dir),
                         trim_blocks=True)

    date_formatted = date.strftime("%Y-%m-%d %H:%M:%S %Z")

    # j2_path = os.path.join(cur_dir, "changelog.md.j2")
    this_change_log_str = j2_env.get_template('changelog.md.j2').render(
            resource=resource_name,
            tag=tag,
            date=date_formatted,
            added=added_files,
            modified=modified_files,
            removed=removed_files
        )

    full_change_log = this_change_log_str + "\n---\n" + previous
    with open(os.path.join(base_path, change_log_name), "w") as f:
        f.write(full_change_log)
    return full_change_log


def get_previous_changes(base_path: Text, change_log_name="README.md"):
    try:
        with open(os.path.join(base_path, change_log_name), "r") as f:
            data = f.read()
        sep = "\n---\n"
        return sep.join(data.split(sep)[1:])
    except FileNotFoundError:
        return ""

