import os
import json
import logging.config


import yaml


def setup_logging(
    config_path='logging.yaml',
    default_level=logging.INFO,
    component=None,
    log_dir="logs"
):

    path = config_path
    if os.path.exists(path):
        print('here')
        with open(path, 'rt') as f:
            # TODO: need a robust implementation
            if '.json' in path:
                config = json.load(f)
            elif '.yaml' in path:
                config = yaml.load(f)
        config['handlers']['info_file_handler']['filename'] = os.path.join(log_dir, "{}.log".format(component))
        config['handlers']['error_file_handler']['filename'] = os.path.join(log_dir, "{}.error".format(component))
        logging.config.dictConfig(config)
    else:
        print('@@')
        logging.basicConfig(level=default_level)