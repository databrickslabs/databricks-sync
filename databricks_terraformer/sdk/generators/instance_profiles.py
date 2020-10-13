from pathlib import Path
from typing import Generator, Dict, Any, Callable

from databricks_cli.sdk import ApiClient
# TODO ommit this via CLOUD_FLAG
from databricks_terraformer.sdk.sync.constants import CloudConstants

from databricks_terraformer.sdk.sync.constants import ResourceCatalog
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.service.instace_profiles import InstanceProfilesService


class InstanceProfileHCLGenerator(APIGenerator):
    INSTANCE_PROFILE_FOR_EACH = "databricks_instance_profiles_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = InstanceProfilesService(self.api_client)
        # TODO no permissions for instance_profile ###  self.__perms = PermissionsHelper(self.api_client)

    def __create_instance_profile_data(self, instance_profile_data: Dict[str, Any],
                                       instance_profile_identifier: Callable[[Dict[str, str]], str]):
        ipd = self._create_data(
            ResourceCatalog.INSTANCE_PROFILE_RESOURCE,
            instance_profile_data,
            lambda: any(
                [self._match_patterns(ud["instance_profile_arn"]) for _, ud in instance_profile_data.items()]) is False,
            instance_profile_identifier,
            instance_profile_identifier,
            self.__make_instance_profile_dict,
            self.map_processors(self.__custom_map_vars)
        )
        ipd.upsert_local_variable(self.INSTANCE_PROFILE_FOR_EACH, instance_profile_data)
        return ipd

    async def _generate(self) -> Generator[APIData, None, None]:
        profiles = self.__service.list_instance_profiles()
        instance_profile_identifier = "databricks_instance_profiles"
        instance_profiles_data = {}
        for profile in profiles.get("instance_profiles", []):
            id_ = profile["instance_profile_arn"]
            this_instance_profile_data = {
                "instance_profile_arn": id_,
            }
            instance_profiles_data[id_] = this_instance_profile_data
        yield self.__create_instance_profile_data(instance_profiles_data,
                                                  lambda x: instance_profile_identifier)

    @property
    def folder_name(self) -> str:
        return "instance_profile"

    def __get_instance_profile_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_instance_profile-{d['instance_profile_arn']}")

    @staticmethod
    def __get_instance_profile_raw_id(data: Dict[str, Any]) -> str:
        return data['instance_profile_arn']

    def __make_instance_profile_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("skip_validation", lambda: False). \
            add_for_each(data, lambda: self.INSTANCE_PROFILE_FOR_EACH, cloud=CloudConstants.AWS). \
            to_dict()
