class PolicyService(object):
    def __init__(self, client):
        self.client = client

    def list_policies(self, headers=None):
        _data = {}

        return self.client.perform_query('GET', '/policies/clusters/list', data=_data, headers=headers)

    def create_policy(self, policy_name, definition, headers=None):
        _data = {}
        if policy_name is not None:
            _data['policy_name'] = policy_name
            # the REST API expects a NAME field not POLICY_NAME
            _data['name'] = policy_name
        if definition is not None:
            _data['definition'] = definition

        return self.client.perform_query('POST', '/policies/clusters/create', data=_data, headers=headers)

    def delete_policy(self, policy_id, headers=None):
        _data = {}
        if policy_id is not None:
            _data['policy_id'] = policy_id
        return self.client.perform_query('POST', '/policies/clusters/delete', data=_data, headers=headers)


    def edit_policy(self, policy_id, policy_name, definition, headers=None):
        _data = {}
        if policy_id is not None:
            _data['policy_id'] = policy_id
        if policy_name is not None:
            _data['policy_name'] = policy_name
        if definition is not None:
            _data['definition'] = definition

        return self.client.perform_query('POST', '/policies/clusters/edit', data=_data, headers=headers)

    def get_policy(self, policy_id, headers=None):
        _data = {}
        if policy_id is not None:
            _data['policy_id'] = policy_id
        return self.client.perform_query('GET', '/policies/clusters/get', data=_data, headers=headers)
