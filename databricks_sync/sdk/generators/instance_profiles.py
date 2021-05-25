from pathlib import Path
from typing import Generator, Dict, Any, Callable, List, Tuple

from databricks_cli.sdk import ApiClient

from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.service.instace_profiles import InstanceProfilesService
from databricks_sync.sdk.sync.constants import CloudConstants, ForEachBaseIdentifierCatalog, \
    InstanceProfileSchema, get_members, GeneratorCatalog
from databricks_sync.sdk.sync.constants import ResourceCatalog


class InstanceProfileHCLGenerator(APIGenerator):
    INSTANCE_PROFILE_FOR_EACH = "databricks_instance_profiles_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = InstanceProfilesService(self.api_client)

    def __create_instance_profile_data(self, instance_profile_data: Dict[str, Any],
                                       instance_profile_identifier: Callable[[Dict[str, str]], str],
                                       for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        # TODO fix the lambda below
        ipd = self._create_data(
            ResourceCatalog.INSTANCE_PROFILE_RESOURCE,
            instance_profile_data,
            lambda: False,
            instance_profile_identifier,
            instance_profile_identifier,
            self.__make_instance_profile_dict,
            self.map_processors(self.__custom_map_vars)
        )
        ipd.upsert_local_variable(self.INSTANCE_PROFILE_FOR_EACH, instance_profile_data)
        ipd.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        return ipd

    async def _generate(self) -> Generator[APIData, None, None]:
        profiles = self.__service.list_instance_profiles()
        instance_profiles_data = {}
        instance_profiles_id_name_pairs = []
        for profile in profiles.get("instance_profiles", []):
            id_ = profile["instance_profile_arn"]
            this_instance_profile_data = {
                InstanceProfileSchema.INSTANCE_PROFILE_ARN: id_,
            }
            instance_profiles_data[id_] = this_instance_profile_data
            instance_profiles_id_name_pairs.append((id_, id_))
        yield self.__create_instance_profile_data(instance_profiles_data, lambda x:
                                                  ForEachBaseIdentifierCatalog.INSTANCE_PROFILES_BASE_IDENTIFIER,
                                                  for_each_var_id_name_pairs=instance_profiles_id_name_pairs)

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.INSTANCE_PROFILE

    def __get_instance_profile_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_instance_profile-{d['instance_profile_arn']}")

    @staticmethod
    def __get_instance_profile_raw_id(data: Dict[str, Any]) -> str:
        return data['instance_profile_arn']

    def __make_instance_profile_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.INSTANCE_PROFILE_RESOURCE). \
            add_for_each(lambda: self.INSTANCE_PROFILE_FOR_EACH, get_members(InstanceProfileSchema),
                         cloud=CloudConstants.AWS). \
            to_dict()
