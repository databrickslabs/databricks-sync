import abc
import copy
from typing import Text, Dict, Optional, Any, Union, List, Tuple

from dotty_dict import Dotty
from pygrok import Grok

from databricks_sync import log
from databricks_sync.sdk.hcl.json_to_hcl import Interpolate
from databricks_sync.sdk.message import HCLConvertData
from databricks_sync.sdk.utils import normalize_identifier


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

    def __init__(self, resource_name: str, dot_path_grok_dict: Dict[Text, Optional[Text]] = None):
        self.resource_name = resource_name
        self.__dot_paths = list(dot_path_grok_dict.keys())
        self.__dot_path_grok_dict = dot_path_grok_dict

    def __sub_grok(self, key, value) -> Optional[str]:
        pattern = self.__dot_path_grok_dict[key] or "%{GREEDYDATA:value}"
        grok = Grok(pattern)
        res: Dict[str, Any] = grok.match(value)
        if res is None:
            return None
        if res is not None and len(list(res.keys())) > 1:
            return None
        for _, groked_val in res.items():
            return groked_val

    @staticmethod
    def __get_resource_value(orig_value, groked_value, variable):
        parameter_wrapped = Interpolate.variable(variable)
        return orig_value.replace(groked_value, parameter_wrapped)

    @staticmethod
    def _generate_keys_and_value(pattern, dotty_dict):
        parts = pattern.split("[*]")
        if len(parts) not in [1, 2]:
            raise ValueError("you can only have 1 wildcard [*]")
        # No wild card path
        if len(parts) == 1:
            try:
                # skip if value is None
                if dotty_dict[pattern] is None:
                    return
                yield pattern, dotty_dict[pattern]
            except KeyError as ke:
                # TODO: change to log later
                # You may set a default map var that does not yet exist
                log.debug(f"Created key error when search for pattern: {pattern} in dictionary: {dotty_dict.to_dict()}")
            finally:
                return
        else:
            first_part = parts[0].rstrip(".")
            try:
                dotty_dict[first_part]
            except KeyError as ke:
                log.debug(f"First part ({first_part}) of the key {pattern} does not exist in dict")
                return

        idx = 0
        while True:
            try:
                key = str(idx).join(parts)
                # skip if value is None
                if dotty_dict[key] is None:
                    return
                yield key, dotty_dict[key]
                idx += 1
            except IndexError:
                break
            except KeyError:
                # key not found in this array slot lets go to next one
                idx += 1

    def __process_map_var_in_dict(self, data: Dict[str, Any],
                                  map_var_dot_path: str,
                                  terraform_model: Union[List[Tuple[str]], HCLConvertData]):
        # Deep copy to avoid mutating the source dictionary
        dotty_data = Dotty(copy.deepcopy(data))
        for key, raw_value in self._generate_keys_and_value(map_var_dot_path, dotty_data):
            final_lines = []
            for line in str(raw_value).split("\n"):
                groked_value = self.__sub_grok(map_var_dot_path, line)
                if groked_value is None:
                    final_lines.append(line)
                    continue
                variable_name = f"{normalize_identifier(groked_value)}"
                final_value = self.__get_resource_value(line, groked_value, variable_name)
                if isinstance(terraform_model, HCLConvertData):
                    terraform_model.add_mapped_variable(variable_name, groked_value)
                if isinstance(terraform_model, list):
                    terraform_model.append((variable_name, groked_value))
                final_lines.append(final_value)
            dotty_data[key] = "\n".join(final_lines)
        return dotty_data.to_dict()

    def __process_latest_version(self, map_var_dot_path, terraform_model: HCLConvertData):
        processed_dictionary = self.__process_map_var_in_dict(terraform_model.latest_version,
                                                              map_var_dot_path,
                                                              terraform_model)
        terraform_model.modify_json(processed_dictionary)

    def __process_all_locals(self, map_var_dot_path, terraform_model: HCLConvertData):
        for local_vars in terraform_model.local_variables:
            if len(local_vars.data.keys()) > 0:
                items = {}
                for local_var, local_data in local_vars.data.items():
                    processed_dictionary = self.__process_map_var_in_dict(local_data, map_var_dot_path, terraform_model)
                    items[local_var] = processed_dictionary
                terraform_model.upsert_local_variable(local_vars.variable_name, {**local_vars.data, **items})

    def process_dict(self, json_dict: Dict[str, Any]):
        list_of_vars = []
        processed_dictionary = copy.deepcopy(json_dict)
        for map_var_dot_path in self.__dot_paths:
            processed_dictionary = self.__process_map_var_in_dict(processed_dictionary,
                                                                  map_var_dot_path,
                                                                  list_of_vars)
        return processed_dictionary, list(set(list_of_vars))

    def _process(self, terraform_model: HCLConvertData):
        for map_var_dot_path in self.__dot_paths:
            if "for_each" in terraform_model.latest_version:
                self.__process_all_locals(map_var_dot_path, terraform_model)
            else:
                self.__process_latest_version(map_var_dot_path, terraform_model)
