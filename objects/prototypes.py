import logs
from config import Config
import os
import sys
from objects.engines import engine


class Prototype:
    def __init__(self, name, kind):
        self.name = name
        self.type = kind

        # Prepare logger
        logs.setup()
        self.logger = logs.logging.getLogger(self.name)
        self.logger.info("[%u] Script run with the following args: %s" %
                         (os.getpid(), sys.argv))

        # args from command prompt
        self.args = sys.argv

        config_section_name = 'sink_name' if kind == 'sink' else 'source_name'

        # Read config
        self.config = Config({config_section_name: self.name}).load()
        self.logger.info("[%u] Config for %s = %s is loaded" %
                         (os.getpid(), config_section_name, self.name))
        self.logger.debug("[%u] %s config is %s" %
                          (os.getpid(), self.name, self.config))

        # connection
        self.connection = engine(self)


class SourcePrototype(Prototype):
    def __init__(self, source_name):
        Prototype.__init__(self, source_name, kind='source')

        # Read file containing sql query
        with open(self.config['file'], 'r', encoding='utf8') as sqlfile:
            sql_query = sqlfile.read().replace('\n', ' ').replace('\t', ' ')
        self.sql = sql_query
        self.logger.info("[%u] Loading SQL query from %s" %
                         (os.getpid(), self.config['file']))
        self.logger.debug("[%u] SQL query is %s" %
                          (os.getpid(), self.sql))

        # Define column to parse as dates
        if 'date_column' in self.config:
            self.column_to_parse_as_dates = [date_column for date_column in self.config['date_column']]
        else:
            self.column_to_parse_as_dates = None
        self.logger.debug("[%u] Column to parse as dates is %s" %
                          (os.getpid(), self.column_to_parse_as_dates))
