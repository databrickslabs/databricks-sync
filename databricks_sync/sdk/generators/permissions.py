import functools
import json
from pathlib import Path
from typing import Callable, Dict, Optional

import requests

from databricks_sync import log
from databricks_sync.sdk.config import export_config
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData, HCLConvertData
from databricks_sync.sdk.processor import MappedGrokVariableBasicAnnotationProcessor
from databricks_sync.sdk.service.permissions import PermissionService
from databricks_sync.sdk.sync.constants import ResourceCatalog, GeneratorCatalog, ForEachBaseIdentifierCatalog, \
    MeConstants
from databricks_sync.sdk.utils import normalize


class NoDirectPermissionsError(ValueError):
    pass


class TerraformPermissionType:
    def __init__(self, object_type, object_id_name, resource_id_field, fetch_actual_value: bool = False):
        self.resource_id_attribute = resource_id_field
        self.object_id_name = object_id_name
        self.object_type = object_type
        self.fetch_actual_value = fetch_actual_value


@functools.lru_cache()
def is_acls_enabled(permissions_service: PermissionService):
    try:
        log.debug("Testing permissions if they exist")
        permissions_service.get_object_permissions("notebooks", "Users")
    except requests.HTTPError as he:
        if he.response.status_code == 500 and "FEATURE_DISABLED" in json.loads(he.response.text).get("message", ""):
            return False

    return True


class PermissionsHelper:

    def __init__(self, api_client):
        self.api_client = api_client
        self._permissions_service = PermissionService(self.api_client)

        self.perm_mapping: Dict[str, 'TerraformPermissionType'] = {
            ResourceCatalog.NOTEBOOK_RESOURCE: TerraformPermissionType("notebooks", "notebook_id", "object_id"),
            ResourceCatalog.DIRECTORY_RESOURCE: TerraformPermissionType("directories", "directory_path", "path",
                                                                        fetch_actual_value=True),
            ResourceCatalog.CLUSTER_POLICY_RESOURCE: TerraformPermissionType("cluster-policies",
                                                                             "cluster_policy_id", "id"),
            ResourceCatalog.INSTANCE_POOL_RESOURCE: TerraformPermissionType("instance-pools",
                                                                            "instance_pool_id", "id"),
            ResourceCatalog.SECRET_SCOPE_RESOURCE: TerraformPermissionType("secret-scope",
                                                                           "name", "id"),
            ResourceCatalog.CLUSTER_RESOURCE: TerraformPermissionType("clusters",
                                                                      "cluster_id", "id"),
            ResourceCatalog.JOB_RESOURCE: TerraformPermissionType("jobs",
                                                                  "job_id", "id")

        }

    @staticmethod
    @normalize
    def _make_identifier(src_object_type, raw_id: str) -> str:
        return f"{src_object_type}-{raw_id}-permissions"

    @staticmethod
    def __make_name(src_obj_data: HCLConvertData):
        return f"{src_obj_data.human_readable_name} permissions"

    @staticmethod
    def _handle_depends_on(tdb: TerraformDictBuilder, additional_depends_on=None):

        depends_on = additional_depends_on or []
        if export_config.contains(GeneratorCatalog.IDENTITY) is True:
            depends_on = depends_on + [
                Interpolate.depends_on(ResourceCatalog.USER_RESOURCE,
                                       ForEachBaseIdentifierCatalog.USERS_BASE_IDENTIFIER),
                Interpolate.depends_on(ResourceCatalog.GROUP_RESOURCE,
                                       ForEachBaseIdentifierCatalog.GROUPS_BASE_IDENTIFIER),
                Interpolate.depends_on(ResourceCatalog.SERVICE_PRINCIPAL_RESOURCE,
                                       ForEachBaseIdentifierCatalog.SERVICE_PRINCIPALS_BASE_IDENTIFIER),
            ]
        if len(depends_on) > 0:
            tdb.add_optional("depends_on", lambda: depends_on)

    def _create_permission_dictionary(self, src_obj_data: HCLConvertData, permission_acls, depends_on=None):
        permission_list = []
        for item in permission_acls:
            for perm in item["all_permissions"]:
                if perm["inherited"] is True:
                    continue
                data = {
                    "group_name": item.get("group_name", None),
                    "user_name": item.get("user_name", None),
                    "service_principal_name": item.get("service_principal_name", None),
                    "permission_level": perm["permission_level"]
                }
                permission_list.append(data)
        if len(permission_list) == 0:
            raise NoDirectPermissionsError("cannot have no acls that are directly attributed")
        perms: TerraformPermissionType = self.perm_mapping[src_obj_data.resource_name]
        tdb = TerraformDictBuilder(ResourceCatalog.PERMISSIONS_RESOURCE, object_name=self.__make_name(src_obj_data))

        # TODO: doc => If cloud dep is not none that means there is a count flag on the parent resource
        # indexed resources get interpolated on value of count
        if perms.fetch_actual_value is False:
            resource_id_attribute = Interpolate.resource(src_obj_data.resource_name,
                                                         src_obj_data.hcl_resource_identifier,
                                                         perms.resource_id_attribute)
        else:
            resource_id_attribute = src_obj_data.latest_version[perms.resource_id_attribute]

        self._handle_depends_on(tdb, additional_depends_on=depends_on)

        tdb. \
            add_required(perms.object_id_name,
                         lambda: resource_id_attribute)
        # If there is only one permission we also need to add a count to skip this (you cannot create a permissions
        # resource that does not have a single access_control block which dynamic makes it empty)
        if len(permission_list) == 1:
            this_principal_owner = permission_list[0].get("user_name") or \
                                   permission_list[0].get("service_principal_name")
            tdb.add_required("count", lambda: Interpolate.count_ternary(
                f'{MeConstants.USERNAME_VAR} != "{this_principal_owner}"'
            ))
        for permission in permission_list:
            this_principal_owner = permission.get("user_name") or permission.get("service_principal_name")
            tdb.add_dynamic_block("access_control", lambda: permission,
                                  custom_ternary_bool_expr=f'{MeConstants.USERNAME_VAR} != '
                                                           f'"{this_principal_owner}"')
        return tdb.to_dict()

    def __filter_inherited_users_dir(self, acl, src_obj_data: HCLConvertData, all_acls):
        u_name = acl.get("user_name", "None")
        path = src_obj_data.latest_version.get("path", "None")
        is_user_home_dir = (Path("/Users") / u_name) == Path(path)
        acls_for_dir = len(all_acls)
        # The default 2 acls are for the user an admins group
        if is_user_home_dir and acls_for_dir == 2:
            log.debug(f"Filtering inherited user directory: {path} for user: {u_name}")
        return not (is_user_home_dir and acls_for_dir == 2)

    def __get_perm_acls(self, src_obj_data: HCLConvertData, perm_data):
        if src_obj_data.resource_name == ResourceCatalog.DIRECTORY_RESOURCE:
            return list(filter(lambda x: self.__filter_inherited_users_dir(x, src_obj_data,
                                                                           perm_data["access_control_list"]),
                               perm_data["access_control_list"]))
        else:
            return perm_data["access_control_list"]

    def create_permission_data(self, src_obj_data: HCLConvertData, path_func: Callable[[str, Optional[str]], Path],
                               rel_path_func: Callable[[str, Optional[str]], str] = None, depends_on=None, ):
        if is_acls_enabled(self._permissions_service) is False:
            raise NoDirectPermissionsError("ACLS are disabled no permissions available")

        identifier = self._make_identifier(src_obj_data.resource_name, src_obj_data.raw_id)
        permissions_name = self.__make_name(src_obj_data)
        parent_local_path = src_obj_data.local_save_path.parent / (identifier + ".tf.json")
        parent_rel_path = str(Path(src_obj_data.relative_save_path).parent / (identifier + ".tf.json")) \
            if src_obj_data.relative_save_path is not None else ""
        log.debug(f"Fetched local parent path for {src_obj_data.resource_name} perms: {parent_local_path}")
        log.debug(f"Fetched relative parent path for {src_obj_data.resource_name} perms: {parent_rel_path}")
        err = None
        try:
            perm_data = self._permissions_service.get_object_permissions(
                self.perm_mapping[src_obj_data.resource_name].object_type, src_obj_data.raw_id)
            permission_acls = self.__get_perm_acls(src_obj_data, perm_data)
            api_data = APIData(
                identifier,
                self.api_client.url,
                identifier,
                self._create_permission_dictionary(src_obj_data, permission_acls, depends_on),
                local_save_path=parent_local_path,
                relative_save_path=parent_rel_path,
                human_readable_name=permissions_name
            )
        except NoDirectPermissionsError as ndpe:
            raise ndpe
        except Exception as e:
            log.exception("Failed to get permisisons")
            api_data = APIData(
                identifier,
                self.api_client.url,
                identifier,
                src_obj_data,
                local_save_path=parent_local_path,
                relative_save_path=parent_rel_path,
                human_readable_name=permissions_name
            )
            err = e
        default_custom_map_vars = {"dynamic.[*].access_control.content.group_name": None,
                                   "dynamic.[*].access_control.content.user_name": None,
                                   "dynamic.[*].access_control.content.service_principal_name": None,
                                   "dynamic.[*].access_control.for_each":
                                       '%{GREEDYDATA} != \\"%{DATA:variable}\\" ? %{GREEDYDATA}',
                                   "count": '%{GREEDYDATA} != \\"%{DATA:variable}\\" ? %{GREEDYDATA}'}
        processors = [] if not export_config.parameterize_permissions else \
            [MappedGrokVariableBasicAnnotationProcessor(ResourceCatalog.PERMISSIONS_RESOURCE,
                                                        dot_path_grok_dict=default_custom_map_vars)]
        hcl_data = HCLConvertData(ResourceCatalog.PERMISSIONS_RESOURCE, api_data,
                                  processors=processors)
        if err is not None:
            hcl_data.add_error(err)
        return hcl_data
