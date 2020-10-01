from pathlib import Path
from typing import Dict, Any

import yaml


class ExportConfig:

    def __init__(self, name, objects: Dict[str, Any] = None):
        self.objects = objects
        self.name = name

    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_dict(cls, dictionary):
        return cls(**dictionary)

    @classmethod
    def from_yaml(cls, path: Path):
        with path.open("r") as f:
            return ExportConfig.from_dict(yaml.load(f, Loader=yaml.SafeLoader))
