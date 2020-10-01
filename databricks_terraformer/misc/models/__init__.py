import abc
import copy
import itertools
import json
from typing import Dict, Text, Any, List, Generator

from databricks_terraformer.sdk.hcl import BLOCK_PREFIX, EXPR_PREFIX
from databricks_terraformer.sdk.hcl.json_to_hcl import create_resource_from_dict, create_variable_from_dict
from databricks_terraformer.internal import APIData
from databricks_terraformer.sdk.utils import normalize_identifier


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
    def visit(self, d, key):
        return get_value_annotated(key, d)


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


def walk_via_dot(old_key_dot: Text, d: Dict[Text, Any],
                 *visitors: DictDotPathVisitor) -> Any:
    keys = old_key_dot.split(".")
    last_key = keys[-1]
    if len(keys) > 1:
        for idx, key in enumerate(keys[:-1]):
            if key == "*" and type(d) == list:
                for list_item in d:
                    return walk_via_dot(".".join(keys[idx + 1:]), list_item, )
            if key not in d:
                raise KeyError(f"key: {key} in {old_key_dot} not found in {d}")
            d = get_value_annotated(key, d)

    # Verify the last key is in there
    # if last_key not in d:
    #     raise KeyError(f"key: {last_key} in {old_key_dot} not found in {d}")
    for visitor in visitors:
        yield visitor.visit(d, last_key)


def consume_walk(it: iter):
    for i in it:
        pass


class Variable:
    def __init__(self, variable_name, default):
        self.default = default
        self.variable_name = variable_name

    def __repr__(self):
        return json.dumps(self.__dict__)

    def to_hcl(self):
        return create_variable_from_dict(self.variable_name, {"default": self.default}, False)


class ResourceHCL:
    def __init__(self, resource_hcl: Text, mapped_vars: List[Variable]):
        self.mapped_vars = mapped_vars
        self.resource_hcl = resource_hcl


class BaseTerraformModel:

    def __init__(self, raw_api_data: APIData):
        self.__identifier = raw_api_data.identifier
        self.__raw_json = raw_api_data.data
        self.__lineage = []
        self.__lineage.append(raw_api_data.data)
        self.__mapped_variables = []
        self.__resource_variables = []

    @property
    def identifer(self):
        return self.__identifier

    @property
    def latest_version(self):
        return self.__lineage[-1]

    @property
    def lineage(self):
        return self.__lineage

    @property
    def mapped_variables(self):
        return self.__mapped_variables

    @property
    def resource_varaibles(self) -> List[Variable]:
        return self.__resource_variables

    def modify_json(self, value):
        self.__lineage.append(value)

    def add_mapped_variable(self, identifer, variable_data):
        self.__mapped_variables.append(Variable(identifer, variable_data))

    def add_resource_variable(self, identifer, variable_data):
        self.__resource_variables.append(Variable(identifer, variable_data))


class Processor(abc.ABC):

    def process(self, terraform_model: BaseTerraformModel):
        self._process(terraform_model)

    @abc.abstractmethod
    def _process(self, terraform_model: BaseTerraformModel):
        pass


class BasicAnnotationProcessor(Processor):

    def __init__(self, prefix: str, dot_paths: List[Text] = None):
        self._dot_paths = dot_paths
        self._prefix = prefix

    def _annotate(self, d) -> None:
        for key in self._dot_paths:
            last_value = key.split(".")[-1]
            new_key = self._prefix + last_value
            visitor = RekeyVisitor(new_key)
            consume_walk(walk_via_dot(key, d, visitor))

    def _process(self, terraform_model: BaseTerraformModel):
        # print(terraform_model)
        this_dict = copy.deepcopy(terraform_model.latest_version)
        self._annotate(this_dict)
        terraform_model.modify_json(this_dict)


class ResourceVariableBasicAnnotationProcessor(BasicAnnotationProcessor):

    def __init__(self, resource_name: str, dot_paths: List[Text] = None):
        super().__init__(EXPR_PREFIX, dot_paths)
        self.resource_name = resource_name

    def __get_resource_variable_name(self, identifier, key):
        return f"{self.resource_name}_{identifier}_{normalize_identifier(key)}"

    def _process(self, terraform_model: BaseTerraformModel):
        for resource_var_dot_path in self._dot_paths:
            this_key = resource_var_dot_path.split(".")[-1]
            variable_name = self.__get_resource_variable_name(terraform_model.identifer, this_key)
            visitor = GetValueVisitor()
            set_value = list(walk_via_dot(resource_var_dot_path, terraform_model.latest_version, visitor))[0]
            terraform_model.add_resource_variable(variable_name, set_value)
            visitor = SetValueVisitor(f"var.{variable_name}")
            consume_walk(walk_via_dot(resource_var_dot_path, terraform_model.latest_version, visitor))
            self._annotate(terraform_model.latest_version)


class MappedVariableBasicAnnotationProcessor(BasicAnnotationProcessor):

    def __init__(self, resource_name: str, dot_paths: List[Text] = None):
        super().__init__(EXPR_PREFIX, dot_paths)
        self.resource_name = resource_name

    def __get_resource_variable_name(self, identifier, key):
        return f"{self.resource_name}_{identifier}_{normalize_identifier(key)}"

    def _process(self, terraform_model: BaseTerraformModel):
        for map_var_dot_path in self._dot_paths:
            visitor = GetValueVisitor()
            value = list(walk_via_dot(map_var_dot_path, terraform_model.latest_version, visitor))[0]
            variable_name = f"{normalize_identifier(value)}"
            terraform_model.add_mapped_variable(variable_name, value)
            visitor = SetValueVisitor(f"var.{variable_name}")
            consume_walk(walk_via_dot(map_var_dot_path, terraform_model.latest_version, visitor))
            self._annotate(terraform_model.latest_version)


class Sink(abc.ABC):

    def push_data(self, terraform_model: BaseTerraformModel):
        gen = self._push_data(terraform_model)
        self.__sink_generator = itertools.chain(self.__sink_generator, gen)

    @abc.abstractmethod
    def _push_data(self, terraform_model: BaseTerraformModel) -> Any:
        pass

    def __init__(self):
        self.__sink_generator = iter({})

    def __iter__(self):
        return self.__sink_generator


class HCLResourceFileSink(Sink):

    def __init__(self, resource_name, debug_mode=False):
        super().__init__()
        self.debug_mode = debug_mode
        self.resource_name = resource_name

    def _push_data(self, terraform_model: BaseTerraformModel) -> Any:
        variable_hcls = "\n\n".join([r_var.to_hcl() for r_var in terraform_model.resource_varaibles])
        resource_hcl = create_resource_from_dict(self.resource_name, terraform_model.identifer,
                                                 terraform_model.latest_version,
                                                 self.debug_mode)
        yield "\n".join([variable_hcls, resource_hcl])


class ApiRespToHCLConverter:

    def __default_generator(self):
        yield APIData(None, None)

    def __init__(self, processors, sinks):
        self.__generator: Generator[APIData] = self.__default_generator()
        self.__processors: List[Processor] = processors
        self.__sinks: List[Sink] = sinks

    def with_processor(self, processor: Processor) -> 'ApiRespToHCLConverter':
        self.__processors.append(processor)
        return self

    def with_sink(self, sink: Sink) -> 'ApiRespToHCLConverter':
        self.__sinks.append(sink)
        return self

    def __set_generator(self, generator) -> 'ApiRespToHCLConverter':
        self.__generator = generator
        return self

    @classmethod
    def builder(cls, generator) -> 'ApiRespToHCLConverter':
        return cls([], []).__set_generator(generator)

    def convert(self):
        for json_value in self.__generator:
            model = BaseTerraformModel(json_value)
            for processor in self.__processors:
                processor.process(model)

            for sink in self.__sinks:
                sink.push_data(model)


def test_generator():
    id = "test"
    file = {"path": "demopath"}
    dbfs_resource_data = {
        "source": f'pathexpand("{id}")',
        "content_b64_md5": f'md5(filebase64(pathexpand("{id}")))',
        "path": file["path"],
        "overwrite": True,
        "mkdirs": True,
        "validate_remote_file": True,
        "instance_type": "m5.xlarge",
        "block": {"block": "test"},
        "cluster_id": "perl-1234232"
    }
    for i in range(1):
        name = file["path"]
        yield APIData(f"{name}-{i}", dbfs_resource_data)


hcl_sink_actual = HCLResourceFileSink("databricks_notebook")

ApiRespToHCLConverter \
    .builder(test_generator()) \
    .with_processor(BasicAnnotationProcessor(BLOCK_PREFIX, dot_paths=["block"])) \
    .with_processor(BasicAnnotationProcessor(EXPR_PREFIX, dot_paths=["source", "content_b64_md5"])) \
    .with_processor(ResourceVariableBasicAnnotationProcessor("databricks_notebook", dot_paths=["cluster_id"])) \
    .with_processor(MappedVariableBasicAnnotationProcessor("databricks_notebook", dot_paths=["instance_type"])) \
    .with_sink(hcl_sink_actual) \
    .convert()

for item in hcl_sink_actual:
    print(item)


class ResourceHCLHandler(abc.ABC):

    def handle(self, resource_hcl_content: str):
        return


class MapVarHCLHandler(abc.ABC):

    def handle(self, map_variable_hcl: str):
        return


class AbstractExporter(abc.ABC):

    def __init__(self, resource_name: Text,
                 block_dot_paths: List[Text] = None,
                 expr_dot_paths: List[Text] = None,
                 raw_str_dot_paths: List[Text] = None,
                 variable_dot_paths: List[Text] = None,
                 map_var_dot_paths: List[Text] = None) -> None:
        self.__resource_name = resource_name
        self.__map_var_dot_paths = map_var_dot_paths
        self.__variable_dot_paths = variable_dot_paths
        self.__raw_str_dot_paths = raw_str_dot_paths
        self.__expr_dot_paths = expr_dot_paths
        self.__block_dot_paths = block_dot_paths
        self.__resource_hcl_handlers: List[ResourceHCLHandler] = []
        self.__map_var_handlers: List[MapVarHCLHandler] = []

    @property
    def resource_name(self):
        return self.__resource_name

    @property
    def map_var_dot_paths(self):
        return self.__map_var_dot_paths

    @property
    def variable_dot_paths(self):
        return self.__variable_dot_paths

    @property
    def raw_str_dot_paths(self):
        return self.__raw_str_dot_paths

    @property
    def expr_dot_paths(self):
        return self.__expr_dot_paths

    @property
    def block_dot_paths(self):
        return self.__block_dot_paths

    def add_resource_hcl_handlers(self, *handlers: ResourceHCLHandler):
        self.__resource_hcl_handlers = self.__resource_hcl_handlers + handlers

    def add_map_var_hcl_handlers(self, *handlers: MapVarHCLHandler):
        self.__map_var_handlers = self.__map_var_handlers + handlers

    @abc.abstractmethod
    def _get_generator(self) -> Generator[APIData]:
        pass

    def __get_generator(self):
        pass

# data = {"@block:test": "demo", "testre":"rekay"}
# for value in walk_via_dot("test", data, RekeyVisitor("@demo:test"), RekeyVisitor("@test:test")):
#     print(value)

# print(data)

# class DatabricksTerraformFactory:
#
#     @staticmethod
#     def notebook(resource_id: Text, data: Dict[Text, Any]):
#         return BaseTerraformModel(DATABRICKS_NOTEBOOK, resource_id, data,
#                                   expr_dot_paths=["source", "content_b64_md5"],
#                                   variable_dot_paths=["path"],
#                                   map_var_dot_paths=["instance_type"])
#
#
# id = "test"
# file = {"path": "demopath"}

#
# btm = DatabricksTerraformFactory.notebook(id, dbfs_resource_data)
# rhcl = btm.to_hcl()
# print(rhcl.resource_hcl)
# for mapped_var in rhcl.mapped_vars:
#     print(mapped_var.to_hcl())
# # rekey_via_dot("test.*.data.test2", "@expr:test2", d)
# # print(d)

# print(files)
