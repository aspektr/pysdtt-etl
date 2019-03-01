import logs
from config import Config
import os


class Prototype:
    def __init__(self, name, kind):
        self.name = name
        self.type = kind

        # Prepare logger
        logs.setup()
        self.logger = logs.logging.getLogger(self.name)

        config_section_name = 'sink_name' if kind == 'sink' else 'source_name'

        # Read config
        self.config = Config({config_section_name: self.name}).load()
        self.logger.info("[%u] Config for %s = %s is loaded" %
                         (os.getpid(), config_section_name, self.name))
        self.logger.debug("[%u] %s config is %s" %
                          (os.getpid(), self.name, self.config))
