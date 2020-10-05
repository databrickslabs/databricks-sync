from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient

from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.generators.permissions import PermissionsHelper
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_terraformer.sdk.service.scim import ScimService
from databricks_terraformer.sdk.utils import normalize_identifier


class IdentityHCLGenerator(APIGenerator):

    @property
    def folder_name(self) -> str:
        return "identity"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = ScimService(self.api_client)
        self.__perms = PermissionsHelper(self.api_client)

    def __create_group_data(self, group_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.GROUP_RESOURCE,
            group_data,
            lambda: any([self._match_patterns(group_data["displayName"])]) is False,
            self.__get_group_identifier,
            self.__get_group_raw_id,
            self.__make_group_dict,
            self.map_processors(self.__custom_map_vars)
        )
    def __create_group_instance_profile_data(self, group_instance_profile_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.GROUP_INSTANCE_PROFILE_RESOURCE,
            group_instance_profile_data,
            lambda: any([self._match_patterns(group_instance_profile_data["displayName"])]) is False,
            self.__get_group_instance_profile_identifier,
            self.__get_group_instance_profile_raw_id,
            self.__make_group_instance_profile_dict,
            self.map_processors(self.__custom_map_vars)
        )
    def __create_member_data(self, member_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.GROUP_MEMBER_RESOURCE,
            member_data,
            lambda: any([self._match_patterns(member_data["displayName"])]) is False,
            self.__get_member_identifier,
            self.__get_member_raw_id,
            self.__make_member_dict,
            self.map_processors(self.__custom_map_vars)
        )

    def __get_user_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data,
                                   lambda d: f"databricks_scim_user-{d['userName']}-{self.__get_user_raw_id(d)}")

    def __get_user_raw_id(self, data: Dict[str, Any]) -> str:
        return data['id']

    def __user_is_admin(self, data: Dict[str, Any]):
        group_is_admin_lst = [True if group["display"] == "admins" else False for group in data.get("groups", [])]
        return lambda: any(group_is_admin_lst)

    def __make_user_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("user_name", lambda: data["userName"]). \
            add_required("default_roles", lambda: []). \
            add_optional("display_name", lambda: data["displayName"]). \
            add_optional("roles", lambda: [valuePair["value"] for valuePair in data["roles"]]). \
            add_optional("entitlements", lambda: [valuePair["value"] for valuePair in data["entitlements"]]). \
            add_optional_if(self.__user_is_admin(data), "set_admin", lambda: True). \
            to_dict()

    def __create_scim_user_data(self, user_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.SCIM_USER_RESOURCE,
            user_data,
            lambda: any([self._match_patterns(user_data["userName"])]) is False,
            self.__get_user_identifier,
            self.__get_user_raw_id,
            self.__make_user_dict,
            self.map_processors(self.__custom_map_vars)
        )

    def __get_group_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_group-{d['displayName']}")

    def __get_group_instance_profile_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_group-{d['displayName']}-{d['value']}")

    def __get_member_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_group-{d['displayName']}-{d['display']}-{d['value']}")

    @staticmethod
    def __get_group_raw_id(data: Dict[str, Any]) -> str:
        return data['id']

    @staticmethod
    def __get_group_instance_profile_raw_id(data: Dict[str, Any]) -> str:
        return data['value']

    @staticmethod
    def __get_member_raw_id(data: Dict[str, Any]) -> str:
        return data['value']

    @staticmethod
    def __make_group_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        allow_cluster_create=False
        allow_instance_pool_create=False
        if "entitlements" in data:
            if lambda: [valuePair["value"] == 'allow-cluster-create' for valuePair in data["entitlements"]]():
                allow_cluster_create=True
            if lambda: [valuePair["value"] == 'allow-instance-pool-create' for valuePair in data["entitlements"]]():
                allow_instance_pool_create=True


        return TerraformDictBuilder(). \
            add_required("display_name", lambda: data["displayName"]). \
            add_required("allow_cluster_create", lambda: allow_cluster_create). \
            add_required("allow_instance_pool_create", lambda: allow_instance_pool_create). \
            to_dict()


    @staticmethod
    def __make_group_instance_profile_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("group_id", lambda: f"databricks_group.databricks_group_{data['displayName']}.id"). \
            add_required("instance_profile_id", lambda: f"databricks_instance_profile.{data['id']}.id"). \
            to_dict()

    @staticmethod
    def __make_member_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("group_id", lambda: f"databricks_group.databricks_group_{data['displayName']}.id"). \
            add_required("member_id", lambda: data["member_id"]). \
            to_dict()


    async def _generate(self) -> Generator[APIData, None, None]:
        user_dict= {}

        service = ScimService(self.api_client)
        users = service.list_users().get("Resources",[])
        for user in users:
            yield self.__create_scim_user_data(user)
            user_dict[user["id"]]=user["userName"]

        groups = service.list_groups()
        for group in groups.get("Resources",[]):
            group_data = self.__create_group_data(group)
            yield group_data

            for group_instance_profile in group.get("roles",[]):
                group_instance_profile["displayName"] = group["displayName"]
                group_instance_profile["id"] = normalize_identifier(f"databricks_instance_profile-{group_instance_profile['value']}")
                group_instance_profile_data = self.__create_group_instance_profile_data(group_instance_profile)
                yield group_instance_profile_data

            for member in group.get("members",[]):
                member["displayName"] = group["displayName"]
                if "Users" in member["$ref"] :
                    member["id"] = normalize_identifier(
                        f"{user_dict[member['value']]}-{member['value']}")
                    member["member_id"] = f"databricks_scim_user.databricks_scim_user_{member['id']}.id"
                else:
                    member["member_id"] = f"databricks_group.databricks_group_{member['displayName']}.id"
                member_data = self.__create_member_data(member)
                yield member_data