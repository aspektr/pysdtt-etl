import yaml
import logs
import os


class Config:
    def __init__(self, config_section: dict, path="configs/config.yaml"):
        self.path = path
        self.config_section = list(config_section.keys())[0]
        self.name = config_section[self.config_section]

        logs.setup()
        self.logger = logs.logging.getLogger(self.name)

    def load(self):
        """Load config file
        :return: dict {'parameter_name': value}
        """
        with open(self.path, 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
            self._check_params(cfg)
        return cfg[self.config_section][self.name]

    def _check_params(self, cfg):

        def check_each_param(self, cfg, list_param):
            try:
                for mandatory_param in list_param:
                    if mandatory_param not in cfg[self.config_section][self.name]:
                        self.logger.exception("[%u] Parameter %s not specified" % (os.getpid(), mandatory_param))
                        raise SystemExit(1)
            except KeyError:
                self.logger.exception("[%u] Can't find %s section or %s in config file" %
                                      (os.getpid(), self.config_section, self.name))

        if self.config_section == 'source_name':
            list_param = 'type', 'host', 'port', 'dbname', 'user', 'psw', 'file', 'cursor_size'
            check_each_param(self, cfg, list_param)

        elif self.config_section == 'sink_name':
            list_param = ('type', 'host', 'port', 'dbname', 'schema',
                          'user', 'psw', 'table', 'if_exists', 'method', 'dtypes')
            check_each_param(self, cfg, list_param)


