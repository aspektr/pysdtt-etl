import logs
from config import Config
import os
import sys
from objects.engines import engine
from objects.engines import get_sink_connection_string


class Prototype:
    def __init__(self, name, kind):
        self.name = name
        self.type = kind

        # Prepare logger
        logs.setup()
        self.logger = logs.logging.getLogger(self.name)
        self.logger.debug("[%u] Instance %s create with the following args: %s" %
                         (os.getpid(), self.name, sys.argv))
        # args from command prompt
        self.args = sys.argv

        config_section_name = 'sink_name' if kind == 'sink' else 'source_name'

        # Read config
        self.config = Config({config_section_name: self.name}).load()
        self.logger.debug("[%u] Config for %s = %s is loaded" %
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


class SinkPrototype(Prototype):
    def __init__(self, sink_name):
        Prototype.__init__(self, sink_name, kind='sink')
        self.cursor = get_sink_connection_string(self).cursor()

        if not self.table_exists():
            self.logger.info("[%u] Table %s doesn't exist. Creating..." %
                             (os.getpid(), self.config['table']))
            self.create_table()

        # TODO add behaviour for if_exists parameter from config.yaml

    def table_exists(self):
        self.logger.debug("[%u] Check the table %s.%s for existing" %
                         (os.getpid(), self.config['schema'], self.config['table']))
        self.cursor.execute("select * from information_schema.tables where table_name='%s'" %
                            self.config['table'])
        self.logger.debug("[%u] Exist? %s" %
                          (os.getpid(), self.cursor.rowcount))
        if self.cursor.rowcount:
            self.logger.debug("[%u] The table '%s' exists" %
                             (os.getpid(), self.config['table']))
        return self.cursor.rowcount

    def create_table(self):
        # TODO add an opportunity (via config) create UNLOGGED TABLE
        # TODO add an opportunity create tabele WITH (autovacuum_enabled=false)
        # This saves CPU time and IO bandwidth on useless vacuuming of the table
        # (since we never DELETE or UPDATE the table).
        ddl = "CREATE TABLE %s.%s (" % (self.config['schema'], self.config['table'])
        for field in self.config['dtypes']:
            ddl += field + " " + self.config['dtypes'][field] + " NULL, "
        ddl += ");"
        ddl = ddl[:-4] + ")"

        self.logger.debug("[%u] Run the following sql %s" %
                          (os.getpid(), ddl))
        self.cursor.execute(ddl)
        self.connection.commit()
        if self.table_exists():
            self.logger.info("[%u] Table's created successfully" % os.getpid())
        else:
            self.logger.info("[%u] Table hasn't created" % os.getpgid())
