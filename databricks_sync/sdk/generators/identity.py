from pathlib import Path
from typing import Generator, Dict, Any, Callable

from databricks_cli.sdk import ApiClient

from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.service.scim import ScimService
from databricks_sync.sdk.sync.constants import ResourceCatalog, CloudConstants, DefaultDatabricksGroups, \
    ForEachBaseIdentifierCatalog, UserSchema, get_members, GroupSchema, GroupInstanceProfileSchema, \
    UserInstanceProfileSchema, GroupMemberSchema, MeConstants
from databricks_sync.sdk.utils import normalize_identifier


def skip_me(local_name):
    return str(f"{{ for k, v in local.{local_name} : k => v " \
           f"if length(regexall({MeConstants.USERNAME_REGEX_VAR}, k)) == 0 }}")

class IdentityHCLGenerator(APIGenerator):
    # static identifiers for the users an groups
    ADMIN_GROUP = "admins"
    USERS_GROUP = "users"
    DEFAULTED_GROUPS = [ADMIN_GROUP, USERS_GROUP]
    USERS_FOREACH_VAR = "databricks_scim_users_for_each_var"
    USER_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE = "databricks_user_instance_profiles_{}_for_each_var"
    GROUPS_FOREACH_VAR = "databricks_groups_for_each_var"
    GROUP_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE = "databricks_group_instance_profiles_{}_for_each_var"
    GROUP_MEMBERS_FOREACH_VAR_TEMPLATE = "databricks_group_members_{}_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars or {}
        self.__service = ScimService(self.api_client)

    @property
    def folder_name(self) -> str:
        return "identity"

    def __create_group_data(self, group_data: Dict[str, Any], groups_identifier: Callable[[Dict[str, str]], str]):
        gd = self._create_data(
            ResourceCatalog.GROUP_RESOURCE,
            group_data,
            lambda: False,
            # lambda: any([self._match_patterns(d["display_name"]) for _, d in group_data.items()]) is False,
            groups_identifier,
            groups_identifier,
            self.__make_group_dict,
            self.map_processors(self.__custom_map_vars)
        )
        # TODO normalize the keys here and interpolate the value
        gd.upsert_local_variable(self.GROUPS_FOREACH_VAR, group_data)
        return gd

    def __create_group_instance_profile_data(self, group_instance_profile_data: Dict[str, Any],
                                             group_instance_profile_identifier: Callable[[Dict[str, str]], str]):
        this_group_instance_profile_id = group_instance_profile_identifier(group_instance_profile_data)
        rd = self._create_data(
            ResourceCatalog.GROUP_INSTANCE_PROFILE_RESOURCE,
            group_instance_profile_data,
            lambda: False,
            group_instance_profile_identifier,
            group_instance_profile_identifier,
            self.__make_group_instance_profile_dict(this_group_instance_profile_id),
            self.map_processors(self.__custom_map_vars)
        )
        rd.upsert_local_variable(self.GROUP_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE.format(
            group_instance_profile_identifier(group_instance_profile_data)),
            group_instance_profile_data)
        return rd

    def __create_member_data(self, member_data: Dict[str, Any], member_identifier: Callable[[Dict[str, str]], str]):
        this_member_id = member_identifier(member_data)
        md = self._create_data(
            ResourceCatalog.GROUP_MEMBER_RESOURCE,
            member_data,
            lambda: False,
            member_identifier,
            member_identifier,
            self.__make_member_dict(this_member_id),
            self.map_processors(self.__custom_map_vars)
        )
        md.upsert_local_variable(self.GROUP_MEMBERS_FOREACH_VAR_TEMPLATE.format(this_member_id),
                                 member_data)
        return md

    def __interpolate_scim_user_id(self, user_name):
        # Short circuit interpolation if it is "Me" as we are skipping this use
        member_interpolation = Interpolate.resource(ResourceCatalog.USER_RESOURCE,
                                    f'{ForEachBaseIdentifierCatalog.USERS_BASE_IDENTIFIER}["{user_name}"]',
                                    'id', wrap_json_syntax=False)
        return Interpolate.ternary(f'"{user_name}" == {MeConstants.USERNAME_VAR}', '"something temp will be skipped"',
                                   member_interpolation)

    def __interpolate_scim_group_id(self, group_name):
        if group_name == self.ADMIN_GROUP:
            return Interpolate.data_source(ResourceCatalog.GROUP_RESOURCE,
                                           DefaultDatabricksGroups.ADMIN_DATA_SOURCE_IDENTIFIER,
                                           DefaultDatabricksGroups.DATA_SOURCE_ID_ATTRIBUTE)
        elif group_name == self.USERS_GROUP:
            return Interpolate.data_source(ResourceCatalog.GROUP_RESOURCE,
                                           DefaultDatabricksGroups.USERS_DATA_SOURCE_IDENTIFIER,
                                           DefaultDatabricksGroups.DATA_SOURCE_ID_ATTRIBUTE)

        return Interpolate.resource(ResourceCatalog.GROUP_RESOURCE,
                                    f'{ForEachBaseIdentifierCatalog.GROUPS_BASE_IDENTIFIER}["{group_name}"]',
                                    'id')

    def __create_user_data(self, user_data: Dict[str, Any],
                           user_identifier: Callable[[Dict[str, str]], str]):
        ud = self._create_data(
            ResourceCatalog.USER_RESOURCE,
            user_data,
            lambda: any([self._match_patterns(d["user_name"]) for _, d in user_data.items()]) is False,
            user_identifier,
            user_identifier,
            self.__make_user_dict,
            self.map_processors(self.__custom_map_vars)
        )
        ud.upsert_local_variable(self.USERS_FOREACH_VAR, user_data)
        return ud

    def __create_user_instance_profile_data(self,
                                            user_name: str,
                                            user_instance_profile_data: Dict[str, Any],
                                            user_instance_profile_identifier: Callable[[Dict[str, str]], str]):
        uipd = self._create_data(
            ResourceCatalog.USER_INSTANCE_PROFILE_RESOURCE,
            user_instance_profile_data,
            lambda: False,
            user_instance_profile_identifier,
            user_instance_profile_identifier,
            self.__make_user_instance_profile_dict(user_name),
            self.map_processors(self.__custom_map_vars)
        )
        uipd.upsert_local_variable(self.USER_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE.format(user_name),
                                   user_instance_profile_data)
        return uipd

    def __make_user_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.USER_RESOURCE). \
            add_for_each(lambda: skip_me(self.USERS_FOREACH_VAR), get_members(UserSchema), just_local=False). \
            to_dict()

    def __make_user_instance_profile_dict(self, user_name: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
        return lambda x: TerraformDictBuilder(ResourceCatalog.USER_INSTANCE_PROFILE_RESOURCE). \
            add_for_each(lambda: skip_me(self.USER_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE.format(user_name)),
                         get_members(UserInstanceProfileSchema), just_local=False). \
            to_dict()

    def __make_group_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.GROUP_RESOURCE). \
            add_for_each(lambda: self.GROUPS_FOREACH_VAR, get_members(GroupSchema)). \
            to_dict()

    def __make_group_instance_profile_dict(self, group_instance_profile_id: str) -> Callable[
        [Dict[str, Any]], Dict[str, Any]]:
        return lambda x: TerraformDictBuilder(ResourceCatalog.GROUP_INSTANCE_PROFILE_RESOURCE). \
            add_for_each(lambda: self.GROUP_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE.format(group_instance_profile_id),
                         get_members(GroupInstanceProfileSchema), cloud=CloudConstants.AWS). \
            to_dict()

    def __make_member_dict(self, member_id: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
        return lambda x: TerraformDictBuilder(ResourceCatalog.GROUP_MEMBER_RESOURCE). \
            add_for_each(lambda: skip_me(self.GROUP_MEMBERS_FOREACH_VAR_TEMPLATE.format(member_id)),
                         get_members(GroupMemberSchema), just_local=False). \
            to_dict()

    def get_user_instance_profiles(self, user):
        user_name = user["userName"]
        user_instance_profile_data = {}
        for instance_profile in user.get("roles", []):
            instance_profile_arn = instance_profile.get("value")
            id_ = f'{user_name}-{instance_profile_arn}'
            user_instance_profile_data[id_] = {
                UserInstanceProfileSchema.USER_ID: self.__interpolate_scim_user_id(user_name),
                UserInstanceProfileSchema.INSTANCE_PROFILE_ID: instance_profile_arn,
            }
        if len(user_instance_profile_data.keys()) > 0:
            return self.__create_user_instance_profile_data(
                user_name,
                user_instance_profile_data, lambda x: normalize_identifier(
                    f"databricks_user_{user_name}-instance_profiles"))
        else:
            return None

    # TODO: a lot of strings things can go sour really fast.
    def get_group_members(self, group, group_name, user_dict):
        group_member_data = {}
        for member in group.get("members", []):
            member_data = {
                GroupMemberSchema.GROUP_ID: self.__interpolate_scim_group_id(group_name),
            }
            # Check if this is a User or a Group
            if "Users/" in member["$ref"]:
                id_ = f"user-{user_dict[member['value']]['userName']}"
                username = user_dict[member["value"]]["userName"]
                member_data[GroupMemberSchema.MEMBER_ID] = self.__interpolate_scim_user_id(username)

            else:
                id_ = f"group-{member['display']}"
                this_memmber_group_name = member["display"]
                member_data[GroupMemberSchema.MEMBER_ID] = self.__interpolate_scim_group_id(this_memmber_group_name)

            group_member_data[id_] = member_data

        if len(group_member_data.keys()) > 0:
            return self.__create_member_data(group_member_data, lambda x: normalize_identifier(
                f"databricks_group_{group_name}-members"))
        else:
            return None

    def get_group_instance_profiles(self, group, group_name):
        group_instance_profiles_data = {}
        for group_instance_profile in group.get("roles", []):
            group_instance_profile_id = group_instance_profile['value']
            if ":instance-profile/" not in group_instance_profile_id:
                continue
            group_instance_profile_data = {
                GroupInstanceProfileSchema.GROUP_ID: self.__interpolate_scim_group_id(group_name),
                GroupInstanceProfileSchema.INSTANCE_PROFILE_ID: group_instance_profile_id
            }
            group_instance_profiles_data[group_instance_profile_id] = group_instance_profile_data
        if len(group_instance_profiles_data.keys()) > 0:
            return self.__create_group_instance_profile_data(group_instance_profiles_data,
                                                             lambda x: normalize_identifier(
                                                                 f"databricks_group_{group_name}"
                                                                 f"-instance-profiles"))
        else:
            return None

    @staticmethod
    def get_user_dict(user):
        display_name = f'{user.get("displayName")}' if user.get("displayName", None) is not None and \
                                                       len(user.get("displayName", "").split(" ")) > 1 else f'Use Email'
        # Modify display if use email, this was done for azure
        if display_name.lower() == "use email":
            display_name = user["userName"]

        entitlements = user.get("entitlements", [])
        allow_cluster_create = any([valuePair["value"] == 'allow-cluster-create' for valuePair in entitlements])
        allow_instance_pool_create = any([valuePair["value"] == 'allow-instance-pool-create'
                                          for valuePair in entitlements])
        return {
            UserSchema.USER_NAME: user["userName"],
            UserSchema.DISPLAY_NAME: display_name,
            UserSchema.ALLOW_CLUSTER_CREATE: allow_cluster_create,
            UserSchema.ALLOW_INSTANCE_POOL_CREATE: allow_instance_pool_create,
            UserSchema.ACTIVE: user["active"]
        }

    @staticmethod
    def get_group_dict(group):
        entitlements = group.get("entitlements", [])
        allow_cluster_create = any([valuePair["value"] == 'allow-cluster-create' for valuePair in entitlements])
        allow_instance_pool_create = any([valuePair["value"] == 'allow-instance-pool-create'
                                          for valuePair in entitlements])
        return {
            GroupSchema.DISPLAY_NAME: group["displayName"],
            GroupSchema.ALLOW_CLUSTER_CREATE: allow_cluster_create,
            GroupSchema.ALLOW_INSTANCE_POOL_CREATE: allow_instance_pool_create,
        }

    async def _generate(self) -> Generator[APIData, None, None]:
        # used to look up the users
        user_lookup_dict = {}
        service = ScimService(self.api_client)

        # requires upfront memory
        users = service.list_users().get("Resources", [])
        groups = service.list_groups().get("Resources", [])

        # Dictionary to create one hcl json file with foreach for groups and users
        user_data = {}
        groups_data = {}
        for user in users:
            id_ = user['userName']
            user_data[id_] = self.get_user_dict(user)
            user_lookup_dict[user["id"]] = user

            user_instance_profiles = self.get_user_instance_profiles(user)
            if user_instance_profiles is not None:
                yield user_instance_profiles

        yield self.__create_user_data(user_data, lambda x: ForEachBaseIdentifierCatalog.USERS_BASE_IDENTIFIER)

        for group in groups:
            id_ = normalize_identifier(group["displayName"])
            if id_ not in self.DEFAULTED_GROUPS:
                groups_data[id_] = self.get_group_dict(group)

            # generate instance profiles and members
            group_name = normalize_identifier(group['displayName'])
            group_instance_profiles = self.get_group_instance_profiles(group, group_name)
            if group_instance_profiles is not None:
                # return group instance profiles for each group
                yield group_instance_profiles
            # If group is users added users to the workspace auto get added here, unable to modify this group
            if id_ != self.USERS_GROUP:
                members = self.get_group_members(group, group_name, user_lookup_dict)
                if members is not None:
                    # return group members for each group
                    yield members
        # return the groups
        yield self.__create_group_data(groups_data, lambda x: ForEachBaseIdentifierCatalog.GROUPS_BASE_IDENTIFIER)
