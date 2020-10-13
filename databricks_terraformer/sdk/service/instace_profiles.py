class InstanceProfilesService(object):
    def __init__(self, client):
        self.client = client

    def list_instance_profiles(self, headers=None):
        _data = {}
        return self.client.perform_query('GET', f"/instance-profiles/list", data=_data,
                                         headers=headers)
