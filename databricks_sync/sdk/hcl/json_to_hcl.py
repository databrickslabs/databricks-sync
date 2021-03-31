import abc
import collections
import copy
import json
from typing import Any, Dict, Callable, List, Union

from databricks_sync import log
from databricks_sync.sdk.sync.constants import CloudConstants


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
    def interpolation_to_resource_name(interpolation: str):
        return interpolation.split(".")[1]

    @staticmethod
    def count_ternary(boolean_expr: str):
        return Interpolate.ternary(boolean_expr, "1", "0")

    @staticmethod
    def ternary(boolean_expr: str, true_expr: Union[str, int], false_expr: Union[str, int]):
        false_expr = str(false_expr).lower() if type(false_expr) == bool else false_expr
        return Expression.wrap(f"{boolean_expr} ? {true_expr} : {false_expr}")

    @staticmethod
    def variable(variable_name: str, wrap_json_syntax=True):
        if wrap_json_syntax is True:
            return f'${{var.{variable_name}}}'
        else:
            return f'var.{variable_name}'

    @staticmethod
    def depends_on(resource_type: str, resource_id: str):
        return f'{resource_type}.{resource_id}'

    @staticmethod
    def resource(resource_type: str, resource_id: str, resource_attrib: str, wrap_json_syntax=True, index=None):
        data = [
            resource_type,
            resource_id if index is None else f'{resource_id}[{index}]',
            resource_attrib
        ]
        interpolated_str = '.'.join(data)
        if wrap_json_syntax is True:
            return Expression.wrap(interpolated_str)
        else:
            return interpolated_str

    @staticmethod
    def data_source(resource_type: str, resource_id: str, resource_attrib: str, wrap_json_syntax=True):
        if wrap_json_syntax is True:
            return Expression.wrap(f'data.{resource_type}.{resource_id}.{resource_attrib}')
        else:
            return f'data.{resource_type}.{resource_id}.{resource_attrib}'


class Expression(TerraformValueWrapper):

    @staticmethod
    def make(field: str, input_dictionary: Dict[str, Any]):
        value = input_dictionary[field]
        if type(value) == str:
            input_dictionary[field] = Expression.wrap(value)
        else:
            raise ValueError(f"value needs to be either a list or a dictionary to be a block instead got: {value} "
                             f"and type: {type(value)}")

    @staticmethod
    def wrap(value):
        return "${" + value + "}"


class TerraformDictBuilder:

    def __init__(self, o_type=None, data=None, **kwargs):
        self.__tf_dict = {}
        self.__base_msg = ""
        # Purely for the purpose of logging
        if o_type is not None:
            self.__base_msg += f"Object: {o_type} "
        for key, value in kwargs.items():
            if callable(value) and data is not None:
                res = value(data)
                if res is not None:
                    self.__base_msg += f"{key}: {res} "
            if isinstance(value, str):
                self.__base_msg += f"{key}: {value} "
        if len(self.__base_msg) > 0:
            self.__base_msg += "=> "

    def add_optional_if(self, condition_func: Callable[[], bool], field: str, value_func: Callable[[], Any],
                        *convertors: TerraformValueWrapper, tf_field_name=None):
        if condition_func() is True:
            self.add_optional(field, value_func, *convertors, tf_field_name=tf_field_name)
        return self

    # TODO: CloudFlag Convertor add that
    def add_optional(self, field: str, value_func: Callable[[], Any],
                     *convertors: TerraformValueWrapper, tf_field_name=None):
        try:
            value = value_func()
            self.__add_field(field, value, tf_field_name=tf_field_name, *convertors)
        except KeyError as e:
            log.debug(self.__base_msg + "permitting optional key error: " + str(e))
        return self

    # the data should map exactly all the fields you want to interpolate directly
    def add_for_each(self, for_each_field_content: Callable[[], Any], schema_key_list: List[str], cloud=None,
                     just_local=True):
        for_each_field_raw: str = for_each_field_content()
        for_each_field_val = f"local.{for_each_field_raw}" if just_local is True else for_each_field_raw

        if cloud is None:
            var_name = for_each_field_val if for_each_field_val.startswith("local.") else for_each_field_val
            self.__add_field("for_each", var_name, Expression())
        else:
            self.__add_field("for_each",
                             f'{CloudConstants.CLOUD_VARIABLE} == "{cloud}" ? {for_each_field_val} : {{}}',
                             Expression())
        for key in schema_key_list:
            self.__add_field(key, f"each.value.{key}", Expression())
        return self

    def add_required(self, field: str, value_func: Callable[[], Any], *convertors: TerraformValueWrapper,
                     tf_field_name=None):
        value = value_func()
        self.__add_field(field, value, tf_field_name=tf_field_name, *convertors)
        return self

    def add_dynamic_blocks(self, field, value_func: Callable[[], Any], cloud_name=None, custom_ternary_bool_expr=None):
        try:
            val = value_func()
            if not isinstance(val, list):
                raise ValueError(f"expected value in field {field} to be a list but got {type(val)}")
            for item in val:
                self.add_dynamic_block(field, lambda: item, cloud_name, custom_ternary_bool_expr)
        except Exception as e:
            log.debug(self.__base_msg + "permitting error: " + str(e))
        return self

    def add_dynamic_block(self, field, value_func: Callable[[], Any], cloud_name=None, custom_ternary_bool_expr=None):
        dynamic_block = {
            field: {
            }
        }
        ternary_expr = f'{CloudConstants.CLOUD_VARIABLE} == "{cloud_name}"' if cloud_name is not None \
            else custom_ternary_bool_expr
        if cloud_name is not None or custom_ternary_bool_expr is not None:
            dynamic_block[field]["for_each"] = \
                Interpolate.ternary(ternary_expr,
                                    "[1]",
                                    "[]")
        else:
            dynamic_block[field]["for_each"] = Expression.wrap("[1]")

        try:
            val = value_func()
            if not isinstance(val, dict):
                raise ValueError(f"expected value in field {field} to be a dictionary but got {val}")
            dynamic_block[field]["content"] = val
            self.__tf_dict.setdefault("dynamic", [])
            self.__tf_dict["dynamic"].append(dynamic_block)
        except KeyError as e:
            log.debug(self.__base_msg + "permitting optional key error: " + str(e))
            log.debug(self.__base_msg + "Dynamic Block Dict: " + str(self.__tf_dict))
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
