import copy
import os


class EnvVarConstants:
    # Debug env vars
    DATABRICKS_SYNC_REPORT_DB_TRACE = "DATABRICKS_SYNC_REPORT_DB_TRACE"
    TF_LOG = "TF_LOG"
    GIT_PYTHON_TRACE = "GIT_PYTHON_TRACE"
    GIT_SSH_COMMAND = "GIT_SSH_COMMAND"

    # SQL Lite info
    DATABRICKS_SYNC_REPORT_DB_DRIVER = "DATABRICKS_SYNC_REPORT_DB_DRIVER"
    DATABRICKS_SYNC_REPORT_DB_URL = "DATABRICKS_SYNC_REPORT_DB_URL"

    # Auth
    AZURE_DATABRICKS_WORKSPACE_ID = "AZURE_DATABRICKS_WORKSPACE_ID"
    DATABRICKS_HOST = "DATABRICKS_HOST"
    DATABRICKS_TOKEN = "DATABRICKS_TOKEN"

    # Passive DR Mode
    TF_VAR_PASSIVE_MODE = "TF_VAR_PASSIVE_MODE"

    # Provider Version Changes
    DATABRICKS_TERRAFORM_PROVIDER_VERSION = "DATABRICKS_TERRAFORM_PROVIDER_VERSION"

    # Import options
    DATABRICKS_SYNC_IMPORT_LOCK = "DATABRICKS_SYNC_IMPORT_LOCK"
    DATABRICKS_SYNC_IMPORT_PLAN_PARALLELISM = "DATABRICKS_SYNC_IMPORT_PLAN_PARALLELISM"
    DATABRICKS_SYNC_IMPORT_APPLY_PARALLELISM = "DATABRICKS_SYNC_IMPORT_APPLY_PARALLELISM"


class ResourceCatalog:
    NOTEBOOK_RESOURCE = "databricks_notebook"
    CLUSTER_POLICY_RESOURCE = "databricks_cluster_policy"
    PERMISSIONS_RESOURCE = "databricks_permissions"
    DBFS_FILE_RESOURCE = "databricks_dbfs_file"
    INSTANCE_POOL_RESOURCE = "databricks_instance_pool"
    INSTANCE_PROFILE_RESOURCE = "databricks_instance_profile"
    SECRET_RESOURCE = "databricks_secret"
    SECRET_SCOPE_RESOURCE = "databricks_secret_scope"
    SECRET_ACL_RESOURCE = "databricks_secret_acl"

    USER_RESOURCE = "databricks_user"
    USER_INSTANCE_PROFILE_RESOURCE = "databricks_user_instance_profile"
    GROUP_RESOURCE = "databricks_group"
    GROUP_INSTANCE_PROFILE_RESOURCE = "databricks_group_instance_profile"
    GROUP_MEMBER_RESOURCE = "databricks_group_member"
    SERVICE_PRINCIPAL_RESOURCE = "databricks_service_principal"

    CLUSTER_RESOURCE = "databricks_cluster"
    JOB_RESOURCE = "databricks_job"


class GeneratorCatalog:
    IDENTITY = "identity"
    CLUSTER_POLICY = "cluster_policy"
    DBFS_FILE = "dbfs_file"
    NOTEBOOK = "notebook"
    INSTANCE_PROFILE = "instance_profile"
    INSTANCE_POOL = "instance_pool"
    SECRETS = "secrets"
    CLUSTER = "cluster"
    JOB = "job"


class ForEachBaseIdentifierCatalog:
    USERS_BASE_IDENTIFIER = "databricks_scim_users"
    SERVICE_PRINCIPALS_BASE_IDENTIFIER = "databricks_scim_service_principals"
    GROUPS_BASE_IDENTIFIER = "databricks_scim_groups"
    DBFS_FILES_BASE_IDENTIFIER = "databricks_dbfs_files"
    INSTANCE_PROFILES_BASE_IDENTIFIER = "databricks_instance_profiles"


class DefaultDatabricksGroups:
    ADMIN_DATA_SOURCE_IDENTIFIER = "admins"
    USERS_DATA_SOURCE_IDENTIFIER = "users"
    DATA_SOURCE_ID_ATTRIBUTE = "id"
    DATA_SOURCE_DEFINITION = {
        ResourceCatalog.GROUP_RESOURCE: {
            ADMIN_DATA_SOURCE_IDENTIFIER: {
                "display_name": "admins"
            },
            USERS_DATA_SOURCE_IDENTIFIER: {
                "display_name": "users"
            }
        }
    }


class TfJsonSchema:
    pass


class DbfsFileSchema(TfJsonSchema):
    PATH = "path"
    SOURCE = "source"


class UserSchema(TfJsonSchema):
    USER_NAME = "user_name"
    DISPLAY_NAME = "display_name"
    ALLOW_CLUSTER_CREATE = "allow_cluster_create"
    ALLOW_INSTANCE_POOL_CREATE = "allow_instance_pool_create"
    ACTIVE = "active"


class GroupSchema(TfJsonSchema):
    DISPLAY_NAME = "display_name"
    ALLOW_CLUSTER_CREATE = "allow_cluster_create"
    ALLOW_INSTANCE_POOL_CREATE = "allow_instance_pool_create"


class ServicePrincipalSchema(TfJsonSchema):
    APPLICATION_ID = "application_id"
    DISPLAY_NAME = "display_name"
    ALLOW_CLUSTER_CREATE = "allow_cluster_create"
    ALLOW_INSTANCE_POOL_CREATE = "allow_instance_pool_create"
    ACTIVE = "active"


class UserInstanceProfileSchema(TfJsonSchema):
    USER_ID = "user_id"
    INSTANCE_PROFILE_ID = "instance_profile_id"


class GroupInstanceProfileSchema(TfJsonSchema):
    GROUP_ID = "group_id"
    INSTANCE_PROFILE_ID = "instance_profile_id"


class GroupMemberSchema(TfJsonSchema):
    GROUP_ID = "group_id"
    MEMBER_ID = "member_id"


class InstanceProfileSchema(TfJsonSchema):
    INSTANCE_PROFILE_ARN = "instance_profile_arn"


class SecretSchema(TfJsonSchema):
    KEY = "key"
    SCOPE = "scope"
    STRING_VALUE = "string_value"


class SecretScopeAclSchema(TfJsonSchema):
    PERMISSION = "permission"
    PRINCIPAL = "principal"
    SCOPE = "scope"


class MeConstants:
    USERNAME_REGEX = "ME_USERNAME_REGEX"
    USERNAME_REGEX_VAR = f"var.{USERNAME_REGEX}"
    USERNAME = "ME_USERNAME"
    USERNAME_VAR = f"var.{USERNAME}"

    @staticmethod
    def set_me_variable(input_dict, username):
        output = copy.deepcopy(input_dict)
        if "variable" not in input_dict:
            output["variable"] = {}
        output["variable"][MeConstants.USERNAME_REGEX] = {
            "default": f"(^|-|_){username}$"
        }
        output["variable"][MeConstants.USERNAME] = {
            "default": f"{username}"
        }
        return output


class SparkEnvConstants:
    SPARK_ENV_INTERNAL_KEY = "@internal:env_var"
    SPARK_ENV_INTERNAL_VALUE = "@internal:env_val"


class CloudConstants:
    AWS = "AWS"
    AZURE = "AZURE"
    CLOUD = "CLOUD"
    CLOUD_VARIABLE = f"upper(var.{CLOUD})"


class DrConstants:
    PASSIVE_MODE = "PASSIVE_MODE"
    PASSIVE_MODE_VARIABLE = f"tobool(var.{PASSIVE_MODE})"


def get_members(klass):
    if not issubclass(klass, TfJsonSchema):
        raise ValueError(f"{type(klass)} should be of type TfJsonSchema")
    return [getattr(klass, attr) for attr in dir(klass)
            if not callable(getattr(klass, attr)) and not attr.startswith("__")]


ENTRYPOINT_MAIN_TF = {
    "provider": {
        "databricks": {
        }
    },
    "terraform": {
        "required_version": ">= 0.13.0",
        "required_providers": {
            "databricks": {
                "source": "databrickslabs/databricks",
                # This should be fixed to not impact this tools behavior when downstream changes are made to the
                # RP. This should be consciously upgraded. Maybe in the future can be passed in as optional
                "version": os.getenv(EnvVarConstants.DATABRICKS_TERRAFORM_PROVIDER_VERSION, "0.3.2")
            }
        }
    },
    "variable": {
        CloudConstants.CLOUD: {},
        DrConstants.PASSIVE_MODE: {
            "default": False
        },
    },
    "data": DefaultDatabricksGroups.DATA_SOURCE_DEFINITION
}
