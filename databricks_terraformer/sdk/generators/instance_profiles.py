from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient

from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.generators.permissions import PermissionsHelper, NoDirectPermissionsError
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.service.instace_profiles import InstanceProfilesService

#TODO ommit this via CLOUD_FLAG


class InstanceProfileHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = InstanceProfilesService(self.api_client)
        #TODO no permissions for instance_profile ###  self.__perms = PermissionsHelper(self.api_client)

    def __create_instance_profile_data(self, instance_profile_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.INSTANCE_PROFILE_RESOURCE,
            instance_profile_data,
            lambda: any([self._match_patterns(instance_profile_data["instance_profile_arn"])]) is False,
            self.__get_instance_profile_identifier,
            self.__get_instance_profile_raw_id,
            self.__make_instance_profile_dict,
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        profiles = self.__service.list_instance_profiles()
        for profile in profiles.get("instance_profiles",[]):
            instance_profile_data = self.__create_instance_profile_data(profile)
            yield instance_profile_data
            #TODO no permissions for instance_profile
            # try:
            #     yield self.__perms.create_permission_data(instance_profile_data, self.get_local_hcl_path)
            # except NoDirectPermissionsError:
        #     pass

    @property
    def folder_name(self) -> str:
        return "instance_profile"

    def __get_instance_profile_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_instance_profile-{d['instance_profile_arn']}")

    @staticmethod
    def __get_instance_profile_raw_id(data: Dict[str, Any]) -> str:
        return data['instance_profile_arn']

    @staticmethod
    def __make_instance_profile_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("instance_profile_arn", lambda: data["instance_profile_arn"]). \
            add_required("skip_validation", lambda: "False"). \
            to_dict()