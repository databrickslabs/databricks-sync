from pathlib import Path
from typing import Generator, Dict, Any

from databricks_cli.sdk import ApiClient

from databricks_terraformer.sdk.generators import ResourceCatalog
from databricks_terraformer.sdk.hcl.json_to_hcl import TerraformDictBuilder, Expression
from databricks_terraformer.sdk.message import APIData
from databricks_terraformer.sdk.pipeline import APIGenerator
from databricks_cli.sdk import SecretService

#TODO ommit this via CLOUD_FLAG


class SecretScopeHCLGenerator(APIGenerator):

    def __init__(self, api_client: ApiClient, base_path: Path, patterns=None,
                 custom_map_vars=None):
        super().__init__(api_client, base_path, patterns=patterns)
        self.__custom_map_vars = custom_map_vars
        self.__service = SecretService(self.api_client)

    def __create_secret_data(self, secret_data: Dict[str, Any]):

        var_name = f"{self.__get_secret_identifier(secret_data)}_var"
        secret_data["var"]  = f"var.{var_name}"

        print(secret_data)
        ret_secret = self._create_data(
            ResourceCatalog.SECRET_RESOURCE,
            secret_data,
            lambda: any([self._match_patterns(secret_data.get("name"))]) is False,
            self.__get_secret_identifier,
            self.__get_secret_raw_id,
            self.__make_secret_dict,
            self.map_processors(self.__custom_map_vars)
        )

        ret_secret.add_resource_variable(var_name)

        return ret_secret

    def __create_secret_scope_data(self, secret_scope_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.SECRET_SCOPE_RESOURCE,
            secret_scope_data,
            lambda: any([self._match_patterns(secret_scope_data.get("name"))]) is False,
            self.__get_secret_scope_identifier,
            self.__get_secret_scope_raw_id,
            self.__make_secret_scope_dict,
            self.map_processors(self.__custom_map_vars)
        )


    def __create_secret_acl_data(self, secret_ACL_data: Dict[str, Any]):
        return self._create_data(
            ResourceCatalog.SECRET_ACL_RESOURCE,
            secret_ACL_data,
            lambda: any([self._match_patterns(secret_ACL_data.get("name"))]) is False,
            self.__get_secret_acl_identifier,
            self.__get_secret_acl_raw_id,
            self.__make_secret_acl_dict,
            self.map_processors(self.__custom_map_vars)
        )

    async def _generate(self) -> Generator[APIData, None, None]:
        secret_scopes = self.__service.list_scopes().get("scopes")
        for secret_scope in secret_scopes:
            secret_scope_data = self.__create_secret_scope_data(secret_scope)
            yield secret_scope_data

            secret_ACLs = self.__service.list_acls(secret_scope.get("name")).get("items")
            for secret_ACL in secret_ACLs:
                secret_ACL["name"] = secret_scope.get("name")
                secret_ACL["scope_id"] = self.__get_secret_scope_identifier(secret_scope)
                secret_scope_acl_data = self.__create_secret_acl_data(secret_ACL)
                yield secret_scope_acl_data

            secrets = self.__service.list_secrets(secret_scope.get("name"))
            if "secrets" in secrets:
                for secret in secrets.get("secrets"):
                    secret["name"] = secret_scope.get("name")
                    secret["scope_id"] = self.__get_secret_scope_identifier(secret_scope)
                    secret_data = self.__create_secret_data(secret)
                    yield secret_data


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
    def __get_secret_acl_raw_id(data: Dict[str, Any],) -> str:
        return data["name"]

    @staticmethod
    def __make_secret_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("key",lambda: data["key"]). \
            add_required("scope",lambda: data["scope_id"]). \
            add_required("string_value",lambda: data["var"],Expression()). \
            to_dict()

    @staticmethod
    def __make_secret_scope_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("name",lambda: data["name"]). \
            add_optional("initial_manage_principal", lambda: data["initial_manage_principal"]). \
            to_dict()

    @staticmethod
    def __make_secret_acl_dict(data: Dict[str, Any]) -> Dict[str, Any]:
        return TerraformDictBuilder(). \
            add_required("scope",lambda: data["scope_id"]). \
            add_required("permission", lambda: data["permission"]). \
            add_required("principal", lambda: data["principal"]). \
            to_dict()


# scope : ${databricks_secret_scope.databricks_secret_scope_admin_tokens.name}