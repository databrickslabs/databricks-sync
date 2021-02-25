import abc
import functools
import json
import traceback
from pathlib import Path
from typing import List, Optional, Any, Dict, Tuple

from databricks_sync import log
from databricks_sync.sdk.hcl.json_to_hcl import TerraformJsonBuilder, \
    TerraformDictBuilder


class Artifact(abc.ABC):
    def __init__(self, remote_path, local_path: Path, service):
        self.local_path = local_path
        self.remote_path = remote_path
        self.service = service

    @abc.abstractmethod
    def get_content(self):
        pass


class APIData:

    def __init__(self, raw_identifier, workspace_url,
                 hcl_resource_identifier, data, local_save_path: Path, relative_save_path: str = None,
                 artifacts: Optional[List[Any]] = None, human_readable_name=None):
        self.__relative_save_path = relative_save_path
        self.__workspace_url = workspace_url
        self.__raw_identifier = raw_identifier
        self.__human_readable_name = human_readable_name if human_readable_name is not None else raw_identifier
        self.__local_save_path = local_save_path
        self.__data = data
        self.__hcl_resource_identifier = hcl_resource_identifier
        self.__artifacts: List[Artifact] = artifacts or []

    def clone_with(self, **kwargs):
        return APIData(**{**self.to_dict(), **kwargs})

    def to_dict(self):
        return {
            "raw_identifier": self.raw_identifier,
            "workspace_url": self.workspace_url,
            "hcl_resource_identifier": self.hcl_resource_identifier,
            "human_readable_name": self.human_readable_name,
            "data": self.data,
            "local_save_path": self.local_save_path,
            "relative_save_path": self.relative_save_path,
            "artifacts": self.artifacts,
        }

    @property
    def relative_save_path(self):
        return self.__relative_save_path

    @property
    def artifacts(self) -> List[Artifact]:
        return self.__artifacts

    @property
    def workspace_url(self):
        return self.__workspace_url

    @property
    def raw_identifier(self):
        return self.__raw_identifier

    @property
    def human_readable_name(self):
        return self.__human_readable_name

    @property
    def hcl_resource_identifier(self):
        return self.__hcl_resource_identifier

    @property
    def data(self):
        return self.__data

    @property
    def local_save_path(self):
        return self.__local_save_path


class Variable:
    def __init__(self, variable_name, default=None):
        self.default = default
        self.variable_name = variable_name

    def __eq__(self, obj):
        return isinstance(obj, Variable) and obj.__dict__ == self.__dict__

    def __repr__(self):
        return json.dumps(self.__dict__)

    def to_dict(self):
        if self.default is None:
            return {}
        else:
            return {
                "default": self.default
            }

    def to_hcl(self, debug: bool):
        tdb = TerraformDictBuilder(). \
            add_optional_if(lambda: self.default is not None, "default", lambda: self.default)
        return TerraformJsonBuilder(). \
            add_variable(self.variable_name, tdb.to_dict()). \
            to_json()


class LocalVariable(Variable):

    def __init__(self, variable_name, data: Dict[str, Any]):
        super().__init__(variable_name)
        self.data = data

    def to_dict(self):
        return {**self.data}


class ErrorMixin:
    def __init__(self):
        self.__errors = []

    def add_error(self, error):
        self.__errors.append(error)

    @property
    def errors(self):
        return self.__errors

    @staticmethod
    def manage_error(func) -> Any:

        @functools.wraps(func)
        def wrapper(inp: ErrorMixin):
            if not isinstance(inp, ErrorMixin):
                return func(inp)

            if isinstance(inp, HCLConvertData):
                log.debug(f"Running: {func.__name__} for {inp.resource_name}: {inp.human_readable_name} with "
                          f"tf id: {inp.hcl_resource_identifier}")
            else:
                log.debug(f"Running: {func.__name__}")
            if len(inp.errors) > 0:
                # This is if the function gets called and an error already exists
                log.info("Found error when processing function: " + func.__name__)
                log.info("Error List: " + str(inp.errors))
                return inp
            try:
                resp = func(inp)
                return resp
            except Exception as e:
                traceback.print_exc()
                inp.add_error(e)
                return inp

        wrapper.managed_error = True
        return wrapper


class HCLConvertData(ErrorMixin):

    def __init__(self, resource_name, raw_api_data: APIData, processors: List['Processor'] = None):
        super().__init__()
        self.__raw_api_data = raw_api_data
        self.__resource_name = resource_name
        self.__processors = processors or []
        self.__lineage = []
        self.__lineage.append(raw_api_data.data)
        self.__mapped_variables = []
        self.__resource_variables = []
        self.__local_variables = {}
        self.__for_each_var_id_name_pairs = []

    @property
    def workspace_url(self):
        return self.__raw_api_data.workspace_url

    @property
    def relative_save_path(self):
        return self.__raw_api_data.relative_save_path

    @property
    def raw_id(self):
        return self.__raw_api_data.raw_identifier

    @property
    def human_readable_name(self):
        return self.__raw_api_data.human_readable_name

    @property
    def local_save_path(self):
        return self.__raw_api_data.local_save_path

    @property
    def resource_name(self):
        return self.__resource_name

    @property
    def artifacts(self) -> List[Artifact]:
        return self.__raw_api_data.artifacts

    @property
    def processors(self):
        return self.__processors

    @property
    def hcl_resource_identifier(self):
        return self.__raw_api_data.hcl_resource_identifier

    @property
    def latest_version(self):
        return self.__lineage[-1]

    @property
    def lineage(self):
        return self.__lineage

    @property
    def mapped_variables(self) -> List[Variable]:
        return self.__mapped_variables

    @property
    def resource_variables(self) -> List[Variable]:
        return self.__resource_variables

    @property
    def local_variables(self) -> List[LocalVariable]:
        return list(self.__local_variables.values())

    @property
    def for_each_var_id_name_pairs(self):
        return self.__for_each_var_id_name_pairs

    def modify_json(self, value):
        self.__lineage.append(value)

    def add_mapped_variable(self, variable_name, variable_default_value):
        self.__mapped_variables.append(Variable(variable_name, variable_default_value))

    def add_resource_variable(self, variable_name, variable_default_value=None):
        self.__resource_variables.append(Variable(variable_name, variable_default_value))

    def upsert_local_variable(self, local_var_name, local_var_value):
        self.__local_variables[local_var_name] = LocalVariable(local_var_name, local_var_value)

    def add_for_each_var_name_pairs(self, pairs: List[Tuple[str, str]]):
        if pairs is not None:
            self.__for_each_var_id_name_pairs += pairs

    def to_hcl(self, debug: bool):
        tjb = TerraformJsonBuilder()
        for r_var in self.resource_variables:
            tjb.add_variable(r_var.variable_name, r_var.to_dict())
        for l_var in self.local_variables:
            tjb.add_locals(l_var.variable_name, l_var.to_dict())
        tjb.add_resource(self.resource_name, self.hcl_resource_identifier, self.latest_version)
        return tjb.to_json()

    @classmethod
    def make_empty_with_error(cls, resource_type, identifier_func, data, err):
        id_ = identifier_func(data)
        hcl_data = cls(resource_type, APIData(id_, None, id_, data, Path(), None), None)
        hcl_data.add_error(err)
        return hcl_data

    def __str__(self):
        return f"{self.hcl_resource_identifier} - {self.errors}"

    @staticmethod
    def process_data(resource_type, data, process_func, id_func):
        try:
            for hcl_convert_data in process_func(data):
                yield hcl_convert_data
        except Exception as e:
            yield HCLConvertData.make_empty_with_error(resource_type, id_func, data, e)
