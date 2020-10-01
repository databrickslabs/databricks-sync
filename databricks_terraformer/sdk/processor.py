import abc
from typing import Text, Dict, Optional

from dotty_dict import Dotty
from pygrok import Grok

from databricks_terraformer.sdk.message import HCLConvertData
from databricks_terraformer.sdk.utils import normalize_identifier


class Processor(abc.ABC):
    """
    The goal of the processor is to provide a function ot modify the json returned from api calls into
    a more hcl looking format to send over to the golang shared lib functions to generate HCL content.
    """

    def process(self, terraform_model: 'HCLConvertData'):
        self._process(terraform_model)

    @abc.abstractmethod
    def _process(self, terraform_model: 'HCLConvertData'):
        pass


class MappedGrokVariableBasicAnnotationProcessor(Processor):
    """
    The MappedGrokVariableBasicAnnotationProcessor converts the given value in the HCL interpolated by a variable and
    exposes it globally in the application as a global variable. This may be things like instance_type, etc that you
    may want to map to other values across environments.
    """

    def __init__(self, resource_name: str, dot_path_grok_dict: Dict[Text, Text] = None):
        self.resource_name = resource_name
        self.__dot_paths = list(dot_path_grok_dict.keys())
        self.__dot_path_grok_dict = dot_path_grok_dict

    def __value_is_interpolated(self, val: str) -> bool:
        return val.startswith("${var.") and val.endswith("}")

    def __sub_grok(self, key, value) -> Optional[str]:
        pattern = self.__dot_path_grok_dict[key] or "%{GREEDYDATA:value}"
        grok = Grok(pattern)
        res = grok.match(value)
        if res is None:
            return None
        if res is not None and len(list(res.keys())) > 1:
            return None
        for _, groked_val in res.items():
            return groked_val

    def __wrap_interpolation(self, value):
        return "${var." + value + "}"

    def __get_resource_value(self, orig_value, groked_value, variable):
        parameter_wrapped = self.__wrap_interpolation(variable)
        return orig_value.replace(groked_value, parameter_wrapped)

    def _generate_keys_and_value(self, pattern, dotty_dict):
        parts = pattern.split("[*]")
        if len(parts) not in [1,2]:
            raise ValueError("you can only have 1 wildcard [*]")
        if len(parts) == 1:
            yield pattern, dotty_dict[pattern]
            return
        idx = 0
        while True:
            try:
                key = str(idx).join(parts)
                yield key, dotty_dict[key]
                idx += 1
            except IndexError:
                break

    def _process(self, terraform_model: HCLConvertData):
        for map_var_dot_path in self.__dot_paths:
            dotty_data = Dotty(terraform_model.latest_version)
            for key, raw_value in self._generate_keys_and_value(map_var_dot_path, dotty_data):
                final_lines = []
                for line in str(raw_value).split("\n"):
                    groked_value = self.__sub_grok(map_var_dot_path, line)
                    if groked_value is None:
                        final_lines.append(line)
                        continue
                    variable_name = f"{normalize_identifier(groked_value)}"
                    final_value = self.__get_resource_value(line, groked_value, variable_name)
                    terraform_model.add_mapped_variable(variable_name, groked_value)
                    final_lines.append(final_value)
                dotty_data[key] = "\n".join(final_lines)
            terraform_model.modify_json(dotty_data.to_dict())
