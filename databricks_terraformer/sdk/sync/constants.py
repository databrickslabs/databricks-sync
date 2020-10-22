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

    CLUSTER_RESOURCE = "databricks_cluster"
    JOB_RESOURCE = "databricks_job"


class DefaultDatabricksAdminGroup:
    DATA_SOURCE_IDENTIFIER = "admins"
    DATA_SOURCE_ATTRIBUTE = "id"
    DATA_SOURCE_DEFINITION = {
        ResourceCatalog.GROUP_RESOURCE: {
            DATA_SOURCE_IDENTIFIER: {
                "display_name": "admins"
            }
        }
    }


class CloudConstants:
    AWS = "AWS"
    AZURE = "AZURE"
    CLOUD_VARIABLE = "var.CLOUD"


ENTRYPOINT_MAIN_TF = {
    "provider": {
        "databricks": {}
    },
    "terraform": {
        "required_version": ">= 0.13.0",
        "required_providers": {
            "databricks": {
                "source": "databrickslabs/databricks",
                # This should be fixed to not impact this tools behavior when downstream changes are made to the
                # RP. This should be consciously upgraded. Maybe in the future can be passed in as optional
                "version": "0.2.7"
            }
        }
    },
    "variable": {
        "CLOUD": {}
    },
    "data": DefaultDatabricksAdminGroup.DATA_SOURCE_DEFINITION
}
