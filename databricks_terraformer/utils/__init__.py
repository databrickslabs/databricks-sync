from typing import List

from pygrok import Grok

valid_resources = [
    "databricks_cluster_policy",
    "databricks_dbfs_file",
    "databricks_instance_pool",
]

def normalize_identifier(identifier):
    return_name=remove_emoji(identifier)
    if identifier[0].isdigit():
        return_name ="_"+identifier

    return re.sub("[^a-zA-Z0-9_]+", "_",return_name)


def handle_block(pool_resource_data, pool, block):
    block_resource_data = {}
    for att in pool[block]:
        block_resource_data[att] = pool[block][att]
    pool_resource_data[f"@block:{block}"] = block_resource_data


def handle_map(pool_resource_data, pool, map):
    block_resource_data = {}
    for att in pool[map]:
        block_resource_data[att] = pool[map][att]
    pool_resource_data[f"{map}"] = block_resource_data


import re


def remove_emoji(text):
    regrex_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002500-\U00002BEF"  # chinese char
                                u"\U00002702-\U000027B0"
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                u"\U0001f926-\U0001f937"
                                u"\U00010000-\U0010ffff"
                                u"\u2640-\u2642"
                                u"\u2600-\u2B55"
                                u"\u200d"
                                u"\u23cf"
                                u"\u23e9"
                                u"\u231a"
                                u"\ufe0f"  # dingbats
                                u"\u3030"
                                "]+", flags=re.UNICODE)

    return regrex_pattern.sub(r'' ,text)

def prep_json(block_key_map, ignore_attribute_key, resource, required_attributes_key):
    for req_key in required_attributes_key:
        assert req_key in resource
    pool_resource_data = {}
    for att in resource:
        if att in ignore_attribute_key:
            log.debug(f"{att} is in ignore list")
            continue

        if att in block_key_map:
            block_key_map[att](pool_resource_data, resource, att)
        else:
            assert type(att) is not dict, f"map/block {att} is not defined"
            pool_resource_data[att] = resource[att]
    return pool_resource_data

class TFGitResource:

    @classmethod
    def from_file_path(cls, path):
        import ntpath
        file_name = ntpath.basename(path)
        # multiple resources may match because of prefix so we want to find the longest matching prefix
        filtered_resources = list(filter(lambda resource: file_name.startswith(resource), valid_resources))
        if len(filtered_resources) is 0:
            return None
        single_resource = max(filtered_resources, key=lambda resource_name: len(resource_name))
        return cls(single_resource, file_name.split(".")[0])

    def __init__(self, resource_type, resource_name):
        self.resource_name = resource_name
        self.resource_type = resource_type

    def __repr__(self):
        return f"resource: {self.resource_type} name: {self.resource_name}"

    def get_plan_target_cmds(self):
        return ["-target", f"{self.resource_type}.{self.resource_name}"]


class TFResource:

    def __init__(self, resource_type, resource_name):
        self.resource_name = resource_name
        self.resource_type = resource_type

    def __repr__(self):
        return f"resource: {self.resource_type} name: {self.resource_name}"


class TFGitResourceFile:

    @classmethod
    def from_file_path(cls, path):
        pattern = '^resource "%{WORD:resource_type}" "%{DATA:resource_name}" {'
        grok = Grok(pattern)
        with open(path, "r") as f:
            lines = f.readlines()

        tf_resources = []

        for line in lines:
            res = grok.match(line)
            if res is not None and "resource_type" in res and "resource_name" in res:
                tf_resources.append(TFResource(**res))
        if len(tf_resources) > 0:
            return cls(tf_resources)
        return cls(tf_resources)

    def __init__(self, resource_list: List[TFResource]):
        self.resource_list = resource_list

    def get_plan_target_cmds(self):
        target_cmds = []
        for resource in self.resource_list:
            target_cmds += ["--target", f"{resource.resource_type}.{resource.resource_name}"]
        return target_cmds
