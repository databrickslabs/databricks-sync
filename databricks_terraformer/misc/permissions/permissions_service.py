class PermissionService(object):
    def __init__(self, client):
        self.client = client


    def get_object_permissions(self, object_type, object_id, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/preview/permissions/{object_type}/{object_id}", data=_data, headers=headers)

    def edit_object_permissions(self, object_type, object_id, entity_permission_dict,  definition, headers=None):
        _data = {}
        for entity,permission in entity_permission_dict:
            _data["access_control_list"][entity] = permission

        return self.client.perform_query('PATCH', f"/preview/permissions/{object_type}/{object_id}", data=_data, headers=headers)

    def set_object_permissions(self, object_type, object_id, entity_permission_dict,  definition, headers=None):
        _data = {}
        for entity,permission in entity_permission_dict:
            _data["access_control_list"][entity] = permission

        return self.client.perform_query('PUT', f"/preview/permissions/{object_type}/{object_id}", data=_data, headers=headers)