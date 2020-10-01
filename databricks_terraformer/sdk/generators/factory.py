import importlib
import inspect
import pkgutil
import re

from databricks_terraformer.sdk import generators
from databricks_terraformer.sdk.pipeline import APIGenerator


class GeneratorFactory:

    def __init__(self, generator_dict):
        self.__generator_dict = generator_dict

    @property
    def generator_map(self):
        return self.__generator_dict

    @staticmethod
    def process_class_name(class_name):
        no_suffix = class_name.replace("HCLGenerator", "")
        pattern = re.compile(r'(?<!^)(?=[A-Z])')
        return pattern.sub('_', no_suffix).lower()

    @classmethod
    def factory(cls):
        package = generators
        generator_mapping = {}
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__):
            module = importlib.import_module(".".join([inspect.getmodule(package).__name__, modname]))
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, APIGenerator) and not inspect.isabstract(obj):
                    generator_mapping[GeneratorFactory.process_class_name(obj.__name__)] = obj
        return cls(generator_mapping)

    def make_generator(self, generator_name, data):
        if generator_name not in self.generator_map:
            raise KeyError(f"unable to find generator with name {generator_name}")
        return self.generator_map[generator_name](**data)
