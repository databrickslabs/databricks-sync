import functools

from databricks_sync import log


class ScimService(object):
    def __init__(self, client):
        self.client = client

    def me(self, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/preview/scim/v2/Me", data=_data,
                                         headers=headers)

    @functools.lru_cache(maxsize=None)
    def list_users(self, headers=None):
        _data = {}
        log.info("Fetching all of the users in the workspace.")
        return self.client.perform_query('GET', f"/preview/scim/v2/Users", data=_data,
                                         headers=headers)

    def get_user_by_id(self, id_, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/preview/scim/v2/Users/{id_}", data=_data,
                                         headers=headers)

    @functools.lru_cache(maxsize=None)
    def list_groups(self, headers=None):
        _data = {}
        log.info("Fetching all of the groups in the workspace.")
        return self.client.perform_query('GET', f"/preview/scim/v2/Groups", data=_data,
                                         headers=headers)

    @functools.lru_cache(maxsize=None)
    def get_group_by_id(self, id, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/preview/scim/v2/Groups/{id}", data=_data,
                                         headers=headers)

    def list_service_principals(self, headers=None):
        _data = {}
        log.info("Fetching all of the service principals in the workspace.")
        return self.client.perform_query('GET', f"/preview/scim/v2/ServicePrincipals", data=_data,
                                         headers=headers)

    def get_service_principal_by_id(self, id_, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/preview/scim/v2/ServicePrincipals/{id_}", data=_data,
                                         headers=headers)
