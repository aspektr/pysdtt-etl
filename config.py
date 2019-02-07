import yaml


class Config:
    def __init__(self, config_section: dict, path="configs/config.yaml"):
        self.path = path
        self.config_section = list(config_section.keys())[0]
        self.name = config_section[self.config_section]

    def load(self):
        """Load config file
        :return: dict {'parameter_name': value}
        """
        with open(self.path, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
        return cfg[self.config_section][self.name]

