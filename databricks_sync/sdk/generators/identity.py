import functools
from pathlib import Path
from typing import Generator, Dict, Any, Callable, List, Tuple

import requests as requests
from databricks_cli.sdk import ApiClient
from databricks_sync import log

from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.service.scim import ScimService
from databricks_sync.sdk.sync.constants import ResourceCatalog, CloudConstants, DefaultDatabricksGroups, \
    ForEachBaseIdentifierCatalog, UserSchema, get_members, GroupSchema, GroupInstanceProfileSchema, \
    UserInstanceProfileSchema, GroupMemberSchema, MeConstants, ServicePrincipalSchema, GeneratorCatalog
from databricks_sync.sdk.utils import normalize_identifier


def skip_me(local_name):
    return str(f"{{ for k, v in local.{local_name} : k => v "
               f"if length(regexall({MeConstants.USERNAME_REGEX_VAR}, k)) == 0 }}")


class IdentityHCLGenerator(APIGenerator):
    # static identifiers for the users an groups
    ADMIN_GROUP = "admins"
    USERS_GROUP = "users"
    DEFAULTED_GROUPS = [ADMIN_GROUP, USERS_GROUP]
    SERVICE_PRINCIPALS_FOREACH_VAR = "databricks_scim_sp_for_each_var"
    USERS_FOREACH_VAR = "databricks_scim_users_for_each_var"
    USER_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE = "databricks_user_instance_profiles_{}_for_each_var"
    GROUPS_FOREACH_VAR = "databricks_groups_for_each_var"
    GROUP_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE = "databricks_group_instance_profiles_{}_for_each_var"
    GROUP_MEMBERS_FOREACH_VAR_TEMPLATE = "databricks_group_members_{}_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None, set_all_users_active=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars or {}
        self.__service: ScimService = ScimService(self.api_client)
        self.__set_all_users_active = set_all_users_active

    @property
    def folder_name(self) -> str:
        return GeneratorCatalog.IDENTITY

    def __create_group_data(self, group_data: Dict[str, Any],
                            groups_identifier: Callable[[Dict[str, str]], str],
                            for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
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
        gd.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        # TODO normalize the keys here and interpolate the value
        gd.upsert_local_variable(self.GROUPS_FOREACH_VAR, group_data)
        return gd

    def __create_group_instance_profile_data(self, group_instance_profile_data: Dict[str, Any],
                                             group_instance_profile_identifier: Callable[[Dict[str, str]], str],
                                             for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
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
        rd.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        rd.upsert_local_variable(self.GROUP_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE.format(
            group_instance_profile_identifier(group_instance_profile_data)),
            group_instance_profile_data)
        return rd

    def __create_member_data(self, member_data: Dict[str, Any],
                             member_identifier: Callable[[Dict[str, str]], str],
                             for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
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
        md.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
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
                                    f'{ForEachBaseIdentifierCatalog.GROUPS_BASE_IDENTIFIER}'
                                    f'["{normalize_identifier(group_name)}"]',
                                    'id')

    def __create_service_principal_data(self, sp_data: Dict[str, Any],
                                        sp_identifier: Callable[[Dict[str, str]], str],
                                        for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        spd = self._create_data(
            ResourceCatalog.SERVICE_PRINCIPAL_RESOURCE,
            sp_data,
            lambda: False,
            sp_identifier,
            sp_identifier,
            self.__make_service_principal_dict,
            self.map_processors(self.__custom_map_vars)
        )
        spd.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        spd.upsert_local_variable(self.SERVICE_PRINCIPALS_FOREACH_VAR, sp_data)
        return spd

    def __make_service_principal_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.USER_RESOURCE). \
            add_for_each(lambda: self.SERVICE_PRINCIPALS_FOREACH_VAR, get_members(ServicePrincipalSchema)). \
            to_dict()

    def __create_user_data(self, user_data: Dict[str, Any],
                           user_identifier: Callable[[Dict[str, str]], str],
                           for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        ud = self._create_data(
            ResourceCatalog.USER_RESOURCE,
            user_data,
            lambda: any([self._match_patterns(d["user_name"]) for _, d in user_data.items()]) is False,
            user_identifier,
            user_identifier,
            self.__make_user_dict,
            self.map_processors(self.__custom_map_vars)
        )
        ud.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        ud.upsert_local_variable(self.USERS_FOREACH_VAR, user_data)
        return ud

    def __create_failed_instance_profile_data(self, display_name: str, arn: str, resource_type: str, err_msg: str):
        identifier = lambda x: f"{resource_type}/{display_name}/arn/{arn}"
        res = {"arn": arn}
        uipd = self._create_data(
            resource_type,
            res,
            lambda: False,
            identifier,
            identifier,
            lambda x: res,
            self.map_processors(self.__custom_map_vars),
            human_readable_name_func=identifier
        )
        uipd.add_error(ValueError(err_msg))
        return uipd


    def __create_user_instance_profile_data(self,
                                            user_name: str,
                                            user_instance_profile_data: Dict[str, Any],
                                            user_instance_profile_identifier: Callable[[Dict[str, str]], str],
                                            for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        normalized_username = normalize_identifier(user_name)
        uipd = self._create_data(
            ResourceCatalog.USER_INSTANCE_PROFILE_RESOURCE,
            user_instance_profile_data,
            lambda: False,
            user_instance_profile_identifier,
            user_instance_profile_identifier,
            self.__make_user_instance_profile_dict(normalized_username),
            self.map_processors(self.__custom_map_vars),
        )
        uipd.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        uipd.upsert_local_variable(self.USER_INSTANCE_PROFILE_FOREACH_VAR_TEMPLATE.format(normalized_username),
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

    @functools.lru_cache(maxsize=False)
    def _group_users_roles(self):
        groups = self.__service.list_groups()
        groups_data = groups["Resources"]
        for group in groups_data:
            if group.get("displayName", None) == "users":
                user_roles = [role.get("value", None) for role in group.get("roles")]
                return user_roles
        return []

    @functools.lru_cache(maxsize=False)
    def _does_group_have_profile(self, group_id, profile_arn) -> bool:
        if group_id is None:
            return False
        group = self.__service.get_group_by_id(group_id)
        if group is None:
            return False
        result = profile_arn in [role.get("value", None) for role in group.get("roles", [{}])]
        if result is True:
            log.debug(f"Group {group.get('displayName', group_id)} has role: {profile_arn} can skip mapping to user!")
        return result

    def _user_group_has_profile(self, user, role_arn):
        # Attempt to be smart about making a role assignment to the user
        try:
            groups_user_belongs_to = user.get("groups", None)
            if groups_user_belongs_to is None:
                return False
            for group in groups_user_belongs_to:
                if self._does_group_have_profile(group.get("value", None), role_arn) is True:
                    return True
            if role_arn in self._group_users_roles():
                log.debug(
                    f"Group users has role: {role_arn} can skip mapping to user!")
                return True
        except Exception:
            log.exception("Failed to check if user group has profile")
            pass
        return False

    def _is_instance_profile_arn(self, arn: str):
        if arn.split(":")[5].startswith("instance-profile"):
            return True
        else:
            log.debug(f"Arn: {arn} is not a valid instance profile arn")
            return False

    def get_user_instance_profiles(self, user):
        user_name = user["userName"]
        errored_arns = []
        user_instance_profile_data = {}
        group_instance_profile_for_each_id_var_pairs = []
        for instance_profile in user.get("roles", []):
            instance_profile_arn = instance_profile.get("value")
            if self._is_instance_profile_arn(instance_profile_arn) is False:
                errored_arns.append(
                    self.__create_failed_instance_profile_data(user_name,
                                                               instance_profile_arn,
                                                               ResourceCatalog.USER_INSTANCE_PROFILE_RESOURCE,
                                                               "Not a valid instance profile arn."))
                continue
            if self._user_group_has_profile(user, instance_profile_arn) is True:
                log.debug(f"Skipping attaching role: {instance_profile_arn} to user: {user_name} because parent group "
                          f"already contains that role.")
                continue
            id_ = f'{user_name}-{instance_profile_arn}'
            user_instance_profile_data[id_] = {
                UserInstanceProfileSchema.USER_ID: self.__interpolate_scim_user_id(user_name),
                UserInstanceProfileSchema.INSTANCE_PROFILE_ID: instance_profile_arn,
            }
            group_instance_profile_for_each_id_var_pairs.append(
                (f'user/{user["id"]}/instance-profile/{instance_profile_arn}',
                 f'user/{user_name}/instance-profile/{instance_profile_arn}'))
        if len(user_instance_profile_data.keys()) > 0:
            return self.__create_user_instance_profile_data(
                user_name,
                user_instance_profile_data, lambda x: normalize_identifier(
                    f"databricks_user_{user_name}-instance_profiles"),
                for_each_var_id_name_pairs=group_instance_profile_for_each_id_var_pairs), errored_arns
        else:
            return None, errored_arns

    # TODO: a lot of strings things can go sour really fast.
    def get_group_members(self, group, group_name, user_dict, service_principal_dict):
        group_member_data = {}
        group_member_for_each_id_var_pairs = []
        group_id = group["id"]
        for member in group.get("members", []):
            member_data = {
                GroupMemberSchema.GROUP_ID: self.__interpolate_scim_group_id(group_name),
            }

            # Check if this is a User or a Group
            if "Users/" in member["$ref"]:
                id_ = f"user-{user_dict[member['value']]['userName']}"
                username = user_dict[member["value"]]["userName"]
                group_member_for_each_id_var_pairs.append(
                    (f'group/{group_id}/user/{user_dict[member["value"]]["id"]}',
                     f'group/{group_name}/user/{user_dict[member["value"]]["userName"]}'))
                member_data[GroupMemberSchema.MEMBER_ID] = self.__interpolate_scim_user_id(username)
            elif "ServicePrincipals/" in member["$ref"]:
                id_ = f"service-principal-{service_principal_dict[member['value']]['applicationId']}"
                application_id = service_principal_dict[member["value"]]["applicationId"]
                group_member_for_each_id_var_pairs.append(
                    (f'group/{group_id}/service-principal/{service_principal_dict[member["value"]]["id"]}',
                     f'group/{group_name}/service-principal/'
                     f'{service_principal_dict[member["value"]]["applicationId"]}'))
                member_data[GroupMemberSchema.MEMBER_ID] = self.__interpolate_scim_user_id(application_id)
            else:
                id_ = f"group-{member['display']}"
                this_member_group_name = member["display"]
                group_member_for_each_id_var_pairs.append(
                    (f'group/{group_id}/group/{member["value"]}',
                     f'group/{group_name}/group/{member["display"]}'))
                member_data[GroupMemberSchema.MEMBER_ID] = self.__interpolate_scim_group_id(this_member_group_name)
            group_member_data[id_] = member_data

        if len(group_member_data.keys()) > 0:
            return self.__create_member_data(group_member_data, lambda x: normalize_identifier(
                f"databricks_group_{group_name}-members"),
                                             for_each_var_id_name_pairs=group_member_for_each_id_var_pairs)
        else:
            return None

    def get_group_instance_profiles(self, group, group_name):
        group_instance_profiles_data = {}
        group_id = group["id"]
        errored_arns = []
        group_instance_profiles_for_each_id_var_pairs = []
        for group_instance_profile in group.get("roles", []):
            group_instance_profile_arn = group_instance_profile['value']
            if self._is_instance_profile_arn(group_instance_profile_arn) is False:
                errored_arns.append(
                    self.__create_failed_instance_profile_data(group.get("displayName", group_id),
                                                               group_instance_profile_arn,
                                                               ResourceCatalog.GROUP_INSTANCE_PROFILE_RESOURCE,
                                                               "Not a valid instance profile arn."))
                continue
            group_instance_profile_data = {
                GroupInstanceProfileSchema.GROUP_ID: self.__interpolate_scim_group_id(group_name),
                GroupInstanceProfileSchema.INSTANCE_PROFILE_ID: group_instance_profile_arn
            }
            group_instance_profiles_data[group_instance_profile_arn] = group_instance_profile_data
            group_instance_profiles_for_each_id_var_pairs.append(
                (f'group/{group_id}/group/{group_instance_profile_arn}',
                 f'group/{group_name}/group/{group_instance_profile_arn}')
            )
        if len(group_instance_profiles_data.keys()) > 0:
            return self.__create_group_instance_profile_data(group_instance_profiles_data,
                                                             lambda x: normalize_identifier(
                                                                 f"databricks_group_{group_name}"
                                                                 f"-instance-profiles"),
                                                             for_each_var_id_name_pairs=
                                                             group_instance_profiles_for_each_id_var_pairs), \
                   errored_arns
        else:
            return None, errored_arns


    def get_user_dict(self, user):
        entitlements = user.get("entitlements", [])
        allow_cluster_create = any([valuePair["value"] == 'allow-cluster-create' for valuePair in entitlements])
        allow_instance_pool_create = any([valuePair["value"] == 'allow-instance-pool-create'
                                          for valuePair in entitlements])
        active = user["active"] if self.__set_all_users_active is None else self.__set_all_users_active
        return {
            UserSchema.USER_NAME: user["userName"],
            # Alternative display name for the user if it does not exist is the username
            UserSchema.DISPLAY_NAME: user.get("displayName", None),
            UserSchema.ALLOW_CLUSTER_CREATE: allow_cluster_create,
            UserSchema.ALLOW_INSTANCE_POOL_CREATE: allow_instance_pool_create,
            UserSchema.ACTIVE: active
        }

    @staticmethod
    def get_service_principal_dict(sp):
        entitlements = sp.get("entitlements", [])
        allow_cluster_create = any([valuePair["value"] == 'allow-cluster-create' for valuePair in entitlements])
        allow_instance_pool_create = any([valuePair["value"] == 'allow-instance-pool-create'
                                          for valuePair in entitlements])
        return {
            ServicePrincipalSchema.APPLICATION_ID: sp["applicationId"],
            ServicePrincipalSchema.DISPLAY_NAME: sp["displayName"],
            ServicePrincipalSchema.ALLOW_CLUSTER_CREATE: allow_cluster_create,
            ServicePrincipalSchema.ALLOW_INSTANCE_POOL_CREATE: allow_instance_pool_create,
            ServicePrincipalSchema.ACTIVE: sp["active"]
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
        service_principal_lookup_dict = {}
        service = ScimService(self.api_client)

        # requires upfront memory
        users = service.list_users().get("Resources", [])
        groups = service.list_groups().get("Resources", [])
        try:
            service_principals = service.list_service_principals().get("Resources", [])
        except requests.HTTPError as he:
            if he.response.status_code == 405 and "Method Not Allowed" in he.response.text:
                log.error("Have to skip service principals due to being disabled in your deployment.")
                service_principals = []
            else:
                log.error("Service principals failed to be retrieved unknown reason. Please investigate.")
                service_principals = []


        # Dictionary to create one hcl json file with foreach for groups and users
        user_data = {}
        groups_data = {}
        service_principals_data = {}

        user_for_each_var_id_name_pairs = []
        sp_for_each_var_id_name_pairs = []
        group_for_each_var_id_name_pairs = []

        for user in users:
            id_ = user['userName']
            user_data[id_] = self.get_user_dict(user)
            user_lookup_dict[user["id"]] = user
            user_for_each_var_id_name_pairs.append((user["id"], user["userName"]))

            user_instance_profiles, errored_arns = self.get_user_instance_profiles(user)
            if user_instance_profiles is not None:
                yield user_instance_profiles
            # report on failed/invalid arns
            for errored_arn in errored_arns:
                yield errored_arn

        yield self.__create_user_data(user_data, lambda x: ForEachBaseIdentifierCatalog.USERS_BASE_IDENTIFIER,
                                      for_each_var_id_name_pairs=user_for_each_var_id_name_pairs)

        for service_principal in service_principals:
            id_ = service_principal['applicationId']
            service_principals_data[id_] = self.get_service_principal_dict(service_principal)
            service_principal_lookup_dict[service_principal["id"]] = service_principal
            sp_for_each_var_id_name_pairs.append((service_principal["id"], service_principal['applicationId']))

        yield self.__create_service_principal_data(service_principals_data,
                                                   lambda
                                                       x: ForEachBaseIdentifierCatalog.SERVICE_PRINCIPALS_BASE_IDENTIFIER,
                                                   for_each_var_id_name_pairs=sp_for_each_var_id_name_pairs)

        for group in groups:
            id_ = normalize_identifier(group["displayName"])
            if id_ not in self.DEFAULTED_GROUPS:
                groups_data[id_] = self.get_group_dict(group)
                group_for_each_var_id_name_pairs.append((group["id"], group["displayName"]))

            # generate instance profiles and members
            group_name = normalize_identifier(group['displayName'])
            group_instance_profiles, errored_arns = self.get_group_instance_profiles(group, group_name)
            if group_instance_profiles is not None:
                # return group instance profiles for each group
                yield group_instance_profiles
            for errored_arn in errored_arns:
                yield errored_arn
            # If group is users added users to the workspace auto get added here, unable to modify this group
            if id_ != self.USERS_GROUP:
                members = self.get_group_members(group, group_name, user_lookup_dict, service_principal_lookup_dict)
                if members is not None:
                    # return group members for each group
                    yield members

        # return the groups
        yield self.__create_group_data(groups_data, lambda x: ForEachBaseIdentifierCatalog.GROUPS_BASE_IDENTIFIER,
                                       for_each_var_id_name_pairs=group_for_each_var_id_name_pairs)
