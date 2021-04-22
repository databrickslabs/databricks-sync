class GlobalInitScriptsService(object):
    def __init__(self, client):
        self.client = client

    def list_global_init_scripts(self, headers=None):
        _data = {}

        return self.client.perform_query('GET', '/global-init-scripts', data=_data, headers=headers)

    def get_global_init_script(self, script_id, headers=None):
        _data = {}
        return self.client.perform_query('GET', f'/global-init-scripts/{script_id}', data=_data, headers=headers)
