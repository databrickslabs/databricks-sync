import io
import json
import os
from typing import Text, Dict, Any

from jinja2 import Environment, FileSystemLoader


def _comment(data):
    arr = []
    for line in data.split("\n"):
        arr.append(f"# {line}")
    return "\n".join(arr)


def create_hcl_file(identity: Text,
                    workspace_url: Text,
                    raw_dict: Dict[Text, Any],
                    hcl_code: Text):
    cur_dir = os.path.dirname(os.path.abspath(__file__))

    hcl_env = Environment(loader=FileSystemLoader(cur_dir),
                          trim_blocks=True)

    return hcl_env.get_template('hcl.tf.j2').render(
        identity=identity,
        workspace_url=workspace_url,
        raw_json=_comment(json.dumps(raw_dict, sort_keys=True, indent=4)),
        hcl_code=hcl_code,
    )
