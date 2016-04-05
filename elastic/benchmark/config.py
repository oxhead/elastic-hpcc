import yaml


class BaseConfig:

    @staticmethod
    def parse_file(config_path):
        with open(config_path, 'r') as f:
            return BaseConfig(yaml.load(f))

    @staticmethod
    def save_file(config, config_path):
        with open(config_path, 'w') as f:
            yaml.dump(config, f)

    def __init__(self, config):
        self.config = config

    def to_file(self, config_path):
        BaseConfig.save_file(self.config, config_path)

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
        return self.lookup_config("controller.host")

    def get_drivers(self):
        return self.lookup_config("driver.hosts")
