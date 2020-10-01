import abc
import collections
import copy
import json
from typing import Any, Dict, Callable


class TerraformValueWrapper(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def make(field: str, input_dictionary: Dict[str, Any]):
        pass


class Block(TerraformValueWrapper):

    @staticmethod
    def make(field: str, input_dictionary: Dict[str, Any]):
        value = input_dictionary[field]
        if isinstance(value, list):
            return
        elif isinstance(value, dict):
            input_dictionary[field] = [value]
        else:
            raise ValueError("value needs to be either a list or a dictionary to be a block")


class Expression(TerraformValueWrapper):

    @staticmethod
    def make(field: str, input_dictionary: Dict[str, Any]):
        value = input_dictionary[field]
        if isinstance(value, str):
            input_dictionary[field] = "${" + value + "}"
        else:
            raise ValueError("value needs to be either a list or a dictionary to be a block")


class TerraformDictBuilder:

    def __init__(self):
        self.__tf_dict = {}

    def add_optional_if(self, condition_func: Callable[[], bool], field: str, value_func: Callable[[], Any],
                     *convertors: TerraformValueWrapper, tf_field_name=None):
        if condition_func():
            self.add_optional(field, value_func, *convertors, tf_field_name=tf_field_name)
        return self

    # TODO: CloudFlag Convertor add that
    def add_optional(self, field: str, value_func: Callable[[], Any],
                     *convertors: TerraformValueWrapper, tf_field_name=None):
        try:
            value = value_func()
            self.__add_field(field, value, tf_field_name=tf_field_name, *convertors)
        except KeyError as e:
            print("permitting optional key error: " + str(e))
        return self

    def add_required(self, field: str, value_func: Callable[[], Any], *convertors: TerraformValueWrapper,
                     tf_field_name=None):
        value = value_func()
        self.__add_field(field, value, tf_field_name=tf_field_name, *convertors)
        return self

    def add_cloud_optional_block(self, field, value_func: Callable[[], Any], cloud_name):
        dynamic_block = {
            field: {
                "for_each": "${var.CLOUD == \"" + cloud_name + "\" ? [1] : []}",
            }
        }
        try:
            val = value_func()
            if not isinstance(val, dict):
                raise ValueError(f"expected value in field {field} to be a dictionary but got {val}")
            dynamic_block[field]["content"] = val
            self.__tf_dict.setdefault("dynamic", [])
            self.__tf_dict["dynamic"].append(dynamic_block)
        except KeyError as e:
            print("permitting optional key error: " + str(e))
        return self

    def __add_field(self, field: str, value: Any, *convertors: TerraformValueWrapper,
                    tf_field_name=None):
        this_field_name = tf_field_name or field
        self.__tf_dict[this_field_name] = value
        for convertor in convertors:
            convertor.make(this_field_name, self.__tf_dict)

    @staticmethod
    def make_cloud_specific(data: Dict[str, Any], cloud_env):
        data["count"] = "${var.CLOUD == \"" + cloud_env + "\" ? 1 : 0}"

    def to_dict(self):
        return self.__tf_dict


class TerraformJsonBuilder:

    def __init__(self):
        self.__variables = collections.OrderedDict()
        self.__resources = collections.OrderedDict()
        self.__outputs = collections.OrderedDict()

    def add_variable(self, variable_name: str, variable: Dict[str, Any]):
        if variable_name in self.__variables:
            raise ValueError("variable already exists")
        self.__variables[variable_name] = variable
        return self

    def add_resource(self, resource_type: str, resource_id: str, resource_content: Dict[str, Any], cloud_flag=None):
        if resource_type not in self.__resources:
            self.__resources[resource_type] = {}
        if resource_id in self.__resources[resource_type]:
            raise ValueError("cannot repeat resource identifier it must be unique across the terraform deployment")
        this_resource_content = copy.deepcopy(resource_content)
        if cloud_flag is not None and isinstance(cloud_flag, str):
            this_resource_content["count"] = "${var.CLOUD == \"" + cloud_flag + "\" ? 1 : 0}"
        self.__resources[resource_type][resource_id] = this_resource_content
        return self

    def to_json(self):
        td = TerraformDictBuilder(). \
            add_optional_if(lambda: len(list(self.__variables.keys())) > 0, "variable", lambda: self.__variables). \
            add_optional_if(lambda: len(list(self.__resources.keys())) > 0, "resource", lambda: self.__resources).\
            to_dict()
        return json.dumps(td, indent=4, sort_keys=True)
