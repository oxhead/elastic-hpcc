import yaml


class BaseConfig:

    @staticmethod
    def parse_file(config_path):
        with open(config_path, 'r') as f:
            return BaseConfig(yaml.load(f))

    def __init__(self, config):
        self.config = config

    def get_config(self, key, default_value=None):
        if key not in self.config:
            return default_value
        else:
            return self.config[key]

    def set_config(self, key, value):
        self.config[key] = value

    def lookup_config(self, key, default_value=None):
        key_list = key.split(".")
        try:
            current_config = self.config
            for k in key_list:
                current_config = current_config[k]
            return current_config
        except Exception as e:
            if default_value is not None:
                return default_value
            raise e

    def get_controller(self):
        return self.get_config("controller")['host']

    def get_drivers(self):
        return self.get_config("driver")['hosts']
