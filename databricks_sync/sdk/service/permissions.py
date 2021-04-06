class PermissionService(object):
    def __init__(self, client):
        self.client = client

    def get_object_permissions(self, object_type, object_id, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/preview/permissions/{object_type}/{object_id}", data=_data,
                                         headers=headers)
