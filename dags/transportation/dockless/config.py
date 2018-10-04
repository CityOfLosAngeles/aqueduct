import os
import yaml

# TODO: Configure to build provider class
class _Config:

    def __init__(self):
        self.path = os.getcwd()
        stream = open(self.path + "/config.yml", 'r')
        data = yaml.load(stream)
        config_keys = data.keys()
        for k in config_keys:
            setattr(self, k, data.get(k))

config = _Config()
