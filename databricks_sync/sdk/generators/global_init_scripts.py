from base64 import b64decode
from pathlib import Path
from typing import List, Generator, Dict, Any, Tuple, Callable

from databricks_cli.sdk import ApiClient

from databricks_sync import log
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_sync.sdk.message import Artifact, APIData
from databricks_sync.sdk.pipeline import DownloaderAPIGenerator
from databricks_sync.sdk.service.global_init_scripts import GlobalInitScriptsService
from databricks_sync.sdk.sync.constants import ResourceCatalog, get_members, GlobalInitScriptSchema, \
    ForEachBaseIdentifierCatalog


class GlobalInitScriptArtifact(Artifact):
    def get_content(self):
        data = self.service.get_global_init_script(self.remote_path)
        return b64decode(data["script"].encode("utf-8"))


class GlobalInitScriptHCLGenerator(DownloaderAPIGenerator):
    GLOBAL_INIT_SCRIPTS_FOREACH_VAR = "databricks_global_init_scripts_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__service = GlobalInitScriptsService(self.api_client)
        self.__custom_map_vars = custom_map_vars or {}

    @property
    def folder_name(self) -> str:
        return "global_init_scripts"

    def _get_global_init_scripts(self):
        resp = self.__service.list_global_init_scripts()
        log.info(f"Fetched all global init scripts")
        if "scripts" not in resp:
            return []
        scripts = resp["scripts"]
        for script in scripts:
            yield script

    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        global_init_scripts = []
        for key in data:
            # Need to inject the script id as the data field doesnt require script_id to generate the hcl json
            local_path = self.get_local_download_path(self.__global_init_script_identifier({**data[key],
                                                                                            "script_id": key}))
            global_init_scripts.append(GlobalInitScriptArtifact(remote_path=key,
                                                                local_path=local_path,
                                                                service=self.__service))
        return global_init_scripts

    def __global_init_script_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_global_init_script-{d['name']}-{d['script_id']}")

    @staticmethod
    def __global_init_script_raw_id(data: Dict[str, Any]) -> str:
        return data['script_id']

    @staticmethod
    def __global_init_script_name(data: Dict[str, Any]) -> str:
        return data.get("name", None)

    def __make_global_init_script_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.GLOBAL_INIT_SCRIPTS_RESOURCE). \
            add_for_each(lambda: self.GLOBAL_INIT_SCRIPTS_FOREACH_VAR, get_members(GlobalInitScriptSchema)). \
            to_dict()

    def __make_global_init_scripts_data(self, gis_data: Dict[str, Any],
                                        gis_identifier: Callable[[Dict[str, str]], str],
                                        for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        gis_data_obj = self._create_data(
            ResourceCatalog.GLOBAL_INIT_SCRIPTS_RESOURCE,
            gis_data,
            # TODO fix this when fixing match_patterns
            lambda: False,
            gis_identifier,
            gis_identifier,
            self.__make_global_init_script_dict,
            self.map_processors(self.__custom_map_vars)
        )
        gis_data_obj.upsert_local_variable(self.GLOBAL_INIT_SCRIPTS_FOREACH_VAR, gis_data)
        gis_data_obj.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        return gis_data_obj

    def __get_global_init_script_dict(self, data: Dict[str, Any], normalize_gis_name: str) -> Dict[str, Any]:
        return {
            GlobalInitScriptSchema.NAME: data["name"],
            GlobalInitScriptSchema.SOURCE: f'${{pathexpand("{normalize_gis_name}")}}',
            GlobalInitScriptSchema.ENABLED: data["enabled"],
            GlobalInitScriptSchema.POSITION: data["position"],
        }

    async def _generate(self) -> Generator[APIData, None, None]:
        global_init_scripts = {}
        global_init_scripts_id_name_pairs = []
        for script in self._get_global_init_scripts():
            id_ = script['script_id']
            global_init_scripts[id_] = self.__get_global_init_script_dict(script,
                                                                          self.__global_init_script_identifier(script))
            # ID and name are same for files
            global_init_scripts_id_name_pairs.append((id_, self.__global_init_script_name(script)))

        if global_init_scripts != {}:
            for_each_identifier = lambda x: ForEachBaseIdentifierCatalog.GLOBAL_INIT_SCRIPTS_BASE_IDENTIFIER
            yield self.__make_global_init_scripts_data(global_init_scripts,
                                                       for_each_identifier,
                                                       for_each_var_id_name_pairs=global_init_scripts_id_name_pairs)
