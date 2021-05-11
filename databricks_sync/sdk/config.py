from pathlib import Path
from typing import Dict, Any

import yaml


class ImmutableSingletonError(Exception):
    pass


class SingletonNotSetError(Exception):
    pass

# TODO: This immutable singleton may need to be changed if we are using beyond cli because
# if its a long living interpreter we may want to change config values
class ExportConfig:
    class __ExportConfigImmutableSingleton:
        def __init__(self, name, objects: Dict[str, Any] = None, parameterize_permissions=None):
            self.objects = objects
            self.name = name
            self._parameterize_permissions = parameterize_permissions or False

        @property
        def parameterize_permissions(self):
            return self._parameterize_permissions

        def contains(self, item: str):
            return item in self.objects

        def to_dict(self):
            return self.__dict__

    __instance = None

    @staticmethod
    def set_from_dict(dictionary) -> '__ExportConfigImmutableSingleton':
        if ExportConfig.__instance is None:
            ExportConfig.__instance = ExportConfig.__ExportConfigImmutableSingleton(**dictionary)
        else:
            raise ImmutableSingletonError("Export Config is an immutable singleton, values should only be set once.")

    @staticmethod
    def read_yaml(path: Path) -> Dict[str, Any]:
        with path.open("r") as f:
            return yaml.load(f, Loader=yaml.SafeLoader)

    @staticmethod
    def set_from_yaml(path: Path) -> '__ExportConfigImmutableSingleton':
        ExportConfig.set_from_dict(ExportConfig.read_yaml(path))

    def __getattr__(self, name):
        # dont go to singleton for these funcs
        if self.__instance is None:
            raise SingletonNotSetError("The singleton for ExportConfig is not set.")
        return getattr(self.__instance, name)


export_config = ExportConfig()