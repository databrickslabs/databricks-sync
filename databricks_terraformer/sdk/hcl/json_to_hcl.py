import abc
import collections
import copy
import json
from typing import Any, Dict, Callable

from databricks_terraformer.sdk.sync.constants import CloudConstants


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


class Interpolate:

    @staticmethod
    def variable(variable_name: str, wrap_json_syntax=True):
        if wrap_json_syntax:
            return f'${{var.{variable_name}}}'
        else:
            return f'var.{variable_name}'

    @staticmethod
    def resource(resource_type: str, resource_id: str, resource_attrib: str, wrap_json_syntax=True):
        if wrap_json_syntax:
            return Expression.wrap(f'{resource_type}.{resource_id}.{resource_attrib}')
        else:
            return f'{resource_type}.{resource_id}.{resource_attrib}'

    @staticmethod
    def data_source(resource_type: str, resource_id: str, resource_attrib: str, wrap_json_syntax=True):
        if wrap_json_syntax:
            return Expression.wrap(f'data.{resource_type}.{resource_id}.{resource_attrib}')
        else:
            return f'data.{resource_type}.{resource_id}.{resource_attrib}'


class Expression(TerraformValueWrapper):

    @staticmethod
    def make(field: str, input_dictionary: Dict[str, Any]):
        value = input_dictionary[field]
        if isinstance(value, str):
            input_dictionary[field] = "${" + value + "}"
        else:
            raise ValueError("value needs to be either a list or a dictionary to be a block")

    @staticmethod
    def wrap(value):
        return "${" + value + "}"


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

    # the data should map exactly all the fields you want to interpolate directly
    def add_for_each(self, data: Dict[str, Dict[str, str]], var_name_func: Callable[[], Any], cloud=None):
        if len(data.keys()) <= 0:
            return self
        var_name_func_val: str = var_name_func()

        if cloud is None:
            var_name = var_name_func_val if var_name_func_val.startswith("local.") else f"local.{var_name_func_val}"
            self.__add_field("for_each", var_name, Expression())
        else:
            self.__add_field("for_each", f'{CloudConstants.CLOUD_VARIABLE} == "{cloud}" ? local.{var_name_func_val} : {{}}',
                             Expression())
        one_item: Dict[str, str] = list(data.values())[0]
        for key, value in one_item.items():
            self.__add_field(key, f"each.value.{key}", Expression())
        return self

    def add_required(self, field: str, value_func: Callable[[], Any], *convertors: TerraformValueWrapper,
                     tf_field_name=None):
        value = value_func()
        self.__add_field(field, value, tf_field_name=tf_field_name, *convertors)
        return self

    def add_cloud_optional_block(self, field, value_func: Callable[[], Any], cloud_name):
        dynamic_block = {
            field: {
                "for_each": f'${{{CloudConstants.CLOUD_VARIABLE} == "{cloud_name}" ? [1] : []}}'
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

    def to_dict(self):
        return self.__tf_dict


class TerraformJsonBuilder:

    def __init__(self):
        self.__locals = collections.OrderedDict()
        self.__variables = collections.OrderedDict()
        self.__resources = collections.OrderedDict()
        self.__outputs = collections.OrderedDict()

    def add_locals(self, local_variable_name: str, local_variable: Dict[str, Any]):
        if local_variable_name in self.__variables:
            raise ValueError("variable already exists")
        self.__locals[local_variable_name] = local_variable
        return self

    def add_variable(self, variable_name: str, variable: Dict[str, Any]):
        if variable_name in self.__variables:
            raise ValueError(f"variable already exists, variable_name is {variable_name} and value is {variable}")
        self.__variables[variable_name] = variable
        return self

    def add_resource(self, resource_type: str, resource_id: str, resource_content: Dict[str, Any], cloud_flag=None):
        if resource_type not in self.__resources:
            self.__resources[resource_type] = {}
        if resource_id in self.__resources[resource_type]:
            raise ValueError("cannot repeat resource identifier it must be unique across the terraform deployment")
        this_resource_content = copy.deepcopy(resource_content)
        if cloud_flag is not None and isinstance(cloud_flag, str):
            this_resource_content["count"] = f'${{{CloudConstants.CLOUD_VARIABLE} == "{cloud_flag}" ? 1 : 0}}'
        self.__resources[resource_type][resource_id] = this_resource_content
        return self

    def to_json(self):
        td = TerraformDictBuilder(). \
            add_optional_if(lambda: len(list(self.__variables.keys())) > 0, "variable", lambda: self.__variables). \
            add_optional_if(lambda: len(list(self.__resources.keys())) > 0, "resource", lambda: self.__resources). \
            add_optional_if(lambda: len(list(self.__locals.keys())) > 0, "locals", lambda: self.__locals). \
            to_dict()
        return json.dumps(td, indent=4, sort_keys=True)
