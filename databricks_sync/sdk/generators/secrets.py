from pathlib import Path
from typing import Generator, Dict, Any, Callable, List, Tuple

from databricks_cli.sdk import ApiClient
from databricks_cli.sdk import SecretService

from databricks_sync.sdk.generators import listify, should_i_process
from databricks_sync.sdk.hcl.json_to_hcl import TerraformDictBuilder, Interpolate
from databricks_sync.sdk.message import APIData
from databricks_sync.sdk.pipeline import APIGenerator
from databricks_sync.sdk.sync.constants import ResourceCatalog, SecretSchema, SecretScopeAclSchema, get_members, \
    SparkEnvConstants, CloudConstants
from databricks_sync.sdk.utils import normalize_identifier


class SecretHCLGenerator(APIGenerator):
    SECRET_SCOPE_ACL_FOREACH_VAR_TEMPLATE = "databricks_secret_scope_{}_acls_for_each_var"
    SECRET_FOREACH_VAR_TEMPLATE = "databricks_secret_{}_for_each_var"

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None, include_scope=None, exclude_scope=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars or {}
        self.__service = SecretService(self.api_client)
        self.__include_scopes = listify(include_scope)
        self.__exclude_scopes = listify(exclude_scope)

    def __create_secret_data(self, scope_name: str, secret_data: Dict[str, Any],
                             variables: List[str],
                             secret_identifier: Callable[[Dict[str, str]], str],
                             for_each_var_id_name_pairs: List[Tuple[str, str]] = None,
                             is_azure: bool = False):
        sd = self._create_data(
            ResourceCatalog.SECRET_RESOURCE,
            secret_data,
            lambda: False,
            secret_identifier,
            secret_identifier,
            self.__make_secret_dict(scope_name, is_azure),
            self.map_processors(self.__custom_map_vars)
        )
        sd.upsert_local_variable(normalize_identifier(self.SECRET_FOREACH_VAR_TEMPLATE.format(scope_name)), secret_data)
        sd.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        for var in variables:
            # If the secret is from an akv secret we want azure to azure to be smooth so we populate this dummy variable
            # The secrets themselves from akv to akv will not be populated and part of the plan
            # But if the variable does not have a default terraform will throw an error so we will populate this stub
            # for the secret value. If you are going from azure -> gcp or azure -> aws you should use TF_VAR to replace
            # this default value
            azure_default_var = {} if is_azure is False else {
                "variable_default_value": "THIS_IS_AN_AZURE_KEY_VAULT_BACKED_SECRET_PLEASE_USE_SPARK_ENV_TO_REPLACE"
            }
            sd.add_resource_variable(var, **azure_default_var)
        return sd

    def __create_secret_scope_data(self, secret_scope_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.SECRET_SCOPE_RESOURCE,
            secret_scope_data,
            lambda: False,
            self.__get_secret_scope_identifier,
            self.__get_secret_scope_raw_id,
            self.__make_secret_scope_dict,
            self.map_processors(self.__custom_map_vars)
        )

    def __create_secret_acl_data(self,
                                 scope_name: str,
                                 secret_acl_data: Dict[str, Any],
                                 acl_identifier: Callable[[Dict[str, str]], str],
                                 for_each_var_id_name_pairs: List[Tuple[str, str]] = None):
        ssad = self._create_data(
            ResourceCatalog.SECRET_ACL_RESOURCE,
            secret_acl_data,
            lambda: False,
            acl_identifier,
            acl_identifier,
            self.__make_secret_acl_dict(scope_name),
            self.map_processors(self.__custom_map_vars)
        )
        ssad.upsert_local_variable(
            normalize_identifier(self.SECRET_SCOPE_ACL_FOREACH_VAR_TEMPLATE.format(scope_name)),
            secret_acl_data
        )
        ssad.add_for_each_var_name_pairs(for_each_var_id_name_pairs)
        return ssad

    def __interpolate_secret_scope(self, scope):
        return Interpolate.resource(ResourceCatalog.SECRET_SCOPE_RESOURCE,
                                    self.__get_secret_scope_identifier(scope),
                                    "id")

    def get_secrets(self, scope, secrets):
        scope_name = self.__get_secret_scope_raw_id(scope)
        is_akv_backed_scope = self.__is_azure_key_vault_scope(scope)
        secret_data = {}
        variables = []
        secrets_id_name_pairs = []
        for secret in secrets:
            id_ = secret["key"]
            var_name = f"{self.get_identifier({}, lambda x: f'{scope_name}_{id_}')}_var"
            secret_data[id_] = {
                SecretSchema.KEY: id_,
                SecretSchema.SCOPE: self.__interpolate_secret_scope(scope),
                SecretSchema.STRING_VALUE: Interpolate.variable(var_name),
                SparkEnvConstants.SPARK_ENV_INTERNAL_KEY: f"TF_VAR_{var_name}",
                SparkEnvConstants.SPARK_ENV_INTERNAL_VALUE: f"{{{{secrets/{scope_name}/{secret['key']}}}}}"
            }
            variables.append(var_name)
            secrets_id_name_pairs.append((f"{scope_name}/{id_}", id_))

        if len(secret_data.keys()) > 0:
            return self.__create_secret_data(scope_name,
                                             secret_data,
                                             variables,
                                             lambda x: normalize_identifier(f"{scope_name}-secrets"),
                                             for_each_var_id_name_pairs=secrets_id_name_pairs,
                                             is_azure=is_akv_backed_scope)
        else:
            return None

    def get_secret_scope_acls(self, scope, secret_scope_acls):
        scope_name = self.__get_secret_scope_raw_id(scope)
        secret_acls_data = {}
        secret_acls_id_name_pairs = []
        for secret_acl in secret_scope_acls:
            permission = secret_acl["permission"]
            principal = secret_acl["principal"]
            acl_id = f'{principal}-{permission}'
            secret_acls_data[acl_id] = {
                SecretScopeAclSchema.PERMISSION: permission,
                SecretScopeAclSchema.PRINCIPAL: principal,
                SecretScopeAclSchema.SCOPE: Interpolate.resource(ResourceCatalog.SECRET_SCOPE_RESOURCE,
                                                                 self.__get_secret_scope_identifier(scope),
                                                                 "id")
            }
            secret_acls_id_name_pairs.append((f"{scope_name}/{acl_id}", acl_id))
        if len(secret_acls_data.keys()) > 0:
            return self.__create_secret_acl_data(scope_name,
                                                 secret_acls_data,
                                                 lambda x: normalize_identifier(f"{scope_name}-acls"),
                                                 for_each_var_id_name_pairs=secret_acls_id_name_pairs)
        else:
            return None

    def __is_azure_key_vault_scope(self, scope):
        return scope.get("backend_type", "DATABRICKS") == "AZURE_KEYVAULT"

    async def _generate(self) -> Generator[APIData, None, None]:
        secret_scopes = self.__service.list_scopes().get("scopes", [])
        for secret_scope in secret_scopes:
            scope_name = self.__get_secret_scope_raw_id(secret_scope)
            # If I am filtering i should just skip all the secrets
            if should_i_process(ResourceCatalog.SECRET_SCOPE_RESOURCE, scope_name, self.__exclude_scopes,
                                self.__include_scopes, "ScopeName") is False:
                continue
            secret_scope_data = self.__create_secret_scope_data(secret_scope)
            yield secret_scope_data

            secret_scope_acls = self.__service.list_acls(secret_scope.get("name")).get("items", [])
            secret_acls = self.get_secret_scope_acls(secret_scope, secret_scope_acls)
            if secret_acls is not None:
                yield secret_acls

            secrets = self.__service.list_secrets(secret_scope["name"]).get("secrets", [])
            secrets_scope_secrets = self.get_secrets(secret_scope, secrets)
            if secrets_scope_secrets is not None:
                yield secrets_scope_secrets

    @property
    def folder_name(self) -> str:
        return "secrets"

    def __get_secret_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_secret-{d['name']}-{d['key']}")

    def __get_secret_scope_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_secret_scope-{d['name']}")

    def __get_secret_acl_identifier(self, data: Dict[str, Any]) -> str:
        return self.get_identifier(data, lambda d: f"databricks_secret_acl-{d['name']}")

    @staticmethod
    def __get_secret_raw_id(data: Dict[str, Any]) -> str:
        return f"{data['name']}-{data['key']}"

    @staticmethod
    def __get_secret_scope_raw_id(data: Dict[str, Any]) -> str:
        return data['name']

    @staticmethod
    def __get_secret_acl_raw_id(data: Dict[str, Any], ) -> str:
        return data["name"]

    def __make_secret_dict(self, scope_name, is_azure: bool) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
        cloud = None if is_azure is not True else CloudConstants.AZURE
        skip_cloud = True if cloud is not None else False
        return lambda x: TerraformDictBuilder(ResourceCatalog.SECRET_RESOURCE). \
            add_for_each(lambda: normalize_identifier(self.SECRET_FOREACH_VAR_TEMPLATE.format(scope_name)),
                         get_members(SecretSchema), cloud=cloud, skip_cloud=skip_cloud). \
            to_dict()

    @staticmethod
    def __make_secret_scope_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(ResourceCatalog.SECRET_SCOPE_RESOURCE). \
            add_required("name", lambda: data["name"]). \
            add_optional("initial_manage_principal", lambda: data["initial_manage_principal"]). \
            add_dynamic_block("keyvault_metadata", lambda: data["keyvault_metadata"],
                              cloud_name=CloudConstants.AZURE). \
            to_dict()

    def __make_secret_acl_dict(self, scope_name) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
        return lambda x: TerraformDictBuilder(ResourceCatalog.SECRET_ACL_RESOURCE). \
            add_for_each(lambda: normalize_identifier(self.SECRET_SCOPE_ACL_FOREACH_VAR_TEMPLATE.format(scope_name)),
                         get_members(SecretScopeAclSchema)). \
            to_dict()
