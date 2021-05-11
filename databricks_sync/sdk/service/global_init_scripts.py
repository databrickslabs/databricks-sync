class GlobalInitScriptsService(object):
    def __init__(self, client):
        self.client = client

    def create_global_init_script(self, data, headers=None):
        return self.client.perform_query('POST', '/global-init-scripts', data=data, headers=headers)

    def list_global_init_scripts(self, headers=None):
        _data = {}

        return self.client.perform_query('GET', '/global-init-scripts', data=_data, headers=headers)

    def get_global_init_script(self, script_id, headers=None):
        _data = {}
        return self.client.perform_query('GET', f'/global-init-scripts/{script_id}', data=_data, headers=headers)

    def delete_global_init_script(self, script_id, headers=None):
        _data = {}
        return self.client.perform_query('DELETE', f'/global-init-scripts/{script_id}', data=_data, headers=headers)
