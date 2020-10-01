import abc
from typing import List, Dict, Any, Text

from pygrok import Grok

from databricks_terraformer import log

valid_resources = [
    "databricks_cluster_policy",
    "databricks_dbfs_file",
    "databricks_notebook",
    "databricks_instance_pool",
    "databricks_instance_profile",
    "databricks_secret_scope",
    "databricks_secret",
    "databricks_secret_acl",
    "jobs",
]


def handle_block(resource_data, object, block):
    block_resource_data = {}
    for att in object[block]:
        block_resource_data[att] = object[block][att]
    resource_data[f"@block:{block}"] = block_resource_data


def handle_map(resource_data, object, map):
    map_resource_data = {}
    for att in object[map]:
        map_resource_data[att] = object[map][att]
    resource_data[f"{map}"] = map_resource_data


def prep_json(block_key_map, ignore_attribute_key, resource, required_attributes_key):
    for req_key in required_attributes_key:
        assert req_key in resource
    pool_resource_data = {}
    for att in resource:
        if att in ignore_attribute_key:
            log.debug(f"{att} is in ignore list")
            continue

        log.debug(att)
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


class Annotation:

    def __init__(self, key, annotation=None):
        self.annotation = annotation
        self.key = key

    @classmethod
    def from_string(cls, val: str) -> 'Annotation':
        parts = val.split(":")
        if len(parts) == 1:
            return cls(parts[0])
        elif len(parts) == 2:
            return cls(parts[1], annotation=parts[0])
        else:
            raise ValueError(f"key: {val} failed to have either have a valid or no annotation or too many semicolons")


class DictDotPathVisitor(abc.ABC):
    @abc.abstractmethod
    def visit(self, d: Dict[Text, Any], key: Text):
        pass


class RekeyVisitor(DictDotPathVisitor):
    def __init__(self, new_key):
        self.new_key = new_key

    def visit(self, d, key):
        d[self.new_key] = pop_value_annotated(key, d)


class SetValueVisitor(DictDotPathVisitor):
    def __init__(self, new_value):
        self.new_value = new_value

    def visit(self, d, key):
        d[get_key_annotated(key, d)] = self.new_value


class GetValueVisitor(DictDotPathVisitor):
    def __init__(self):
        self.__values = []

    def visit(self, d, key):
        v = get_value_annotated(key, d)
        self.__values.append(v)

    @property
    def values(self):
        return self.__values

def pop_value_annotated(k: str, d: Dict[Text, Any]):
    keys = list(d.keys())
    unannotated_keys = [key.split(":")[-1] for key in keys]
    for idx, key in enumerate(unannotated_keys):
        if k == key:
            return d.pop(keys[idx])
    raise KeyError(f"key: {k} not found in {d}")


def get_key_annotated(k: str, d: Dict[Text, Any]):
    keys = list(d.keys())
    unannotated_keys = [key.split(":")[-1] for key in keys]
    for idx, key in enumerate(unannotated_keys):
        if k == key:
            return keys[idx]
    raise KeyError(f"key: {k} not found in {d}")


def get_value_annotated(k: str, d: Dict[Text, Any]):
    keys = list(d.keys())
    unannotated_keys = [key.split(":")[-1] for key in keys]
    for idx, key in enumerate(unannotated_keys):
        if k == key:
            return d[keys[idx]]
    raise KeyError(f"key: {k} not found in {d}")


def is_array(key: str) -> bool:
    return key.startswith("[") and key.endswith("]")


def get_array_value(key):
    return key.replace("[", "").replace("]", "")


def walk_via_dot(old_key_dot: Text, d: Dict[Text, Any],
                 *visitors: DictDotPathVisitor) -> Any:
    keys = old_key_dot.split(".")
    last_key = keys[-1]
    if len(keys) > 1:
        for idx, key in enumerate(keys[:-1]):
            if is_array(key) and type(d) == list:
                if get_array_value(key) == "*":
                    for list_item in d:
                        walk_via_dot(".".join(keys[idx + 1:]), list_item, *visitors)
                elif int(get_array_value(key)) < len(d):
                    walk_via_dot(".".join(keys[idx + 1:]), d[int(get_array_value(key))], *visitors)
                else:
                    raise IndexError(f"cannot look for {get_array_value(key)} in list {d}")
                return
            # print(d)
            if key not in d.keys() and key not in [key.split(":")[-1] for key in d.keys()]:
                raise KeyError(f"key: {key} in {old_key_dot} not found in {d}")
            d = get_value_annotated(key, d)

    for visitor in visitors:

        visitor.visit(d, last_key)


    # def walk_via_dot(old_key_dot: Text, d: Dict[Text, Any],
#                  *visitors: DictDotPathVisitor) -> Any:
#     keys = old_key_dot.split(".")
#     last_key = keys[-1]
#     if len(keys) > 1:
#         for idx, key in enumerate(keys[:-1]):
#             if is_array(key) and type(d) == list:
#                 if get_array_value(key) == "*":
#                     for list_item in d:
#                         walk_via_dot(".".join(keys[idx + 1:]), list_item, *visitors)
#                 elif int(get_array_value(key)) < len(d):
#                     walk_via_dot(".".join(keys[idx + 1:]), d[int(get_array_value(key))], *visitors)
#                 else:
#                     raise IndexError(f"cannot look for {get_array_value(key)} in list {d}")
#                 return
#             if key not in d.keys() and key not in [key.split(":")[-1] for key in d.keys()]:
#                 raise KeyError(f"key: {key} in {old_key_dot} not found in {d}")
#             d = get_value_annotated(key, d)
#
#     # :
#     return [visitor.visit(d, last_key) for visitor in visitors]
