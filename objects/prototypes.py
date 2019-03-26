import logs
from config import Config
import os
import sys
from objects.engines import engine
from objects.engines import get_sink_connection_string
from objects.query_reader import read_query
from pandas import read_sql_query
from objects.engines import get_pandas_connection_string


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

        # Read file containing query
        self.query = read_query(self)
        self.logger.debug("[%u] Query is %s" %
                          (os.getpid(), self.query))


class SourcePrototype(Prototype):
    def __init__(self, source_name):
        Prototype.__init__(self, source_name, kind='source')

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

        self.metadata = self.get_table_metadata()

        new_cols = self.check_col()
        if new_cols != set():
            dtypes = {k.lower(): self.config['dtypes'][k] for k in self.config['dtypes']}
            for col in new_cols:
                self.create_column(col, dtypes[col])

        # TODO add behaviour for if_exists parameter from config.yaml

    def table_exists(self):
        self.logger.debug("[%u] Check the table %s.%s for existing" %
                          (os.getpid(), self.config['schema'], self.config['table']))
        self.cursor.execute("select * from information_schema.tables where table_name='%s' and table_schema='%s'" %
                            (self.config['table'], self.config['schema']))
        self.logger.debug("[%u] Exist? %s" %
                          (os.getpid(), self.cursor.rowcount))
        if self.cursor.rowcount:
            self.logger.debug("[%u] The table '%s' exists" %
                              (os.getpid(), self.config['table']))
        return self.cursor.rowcount

    def create_table(self):
        # TODO add an opportunity (via config) create UNLOGGED TABLE
        # TODO add an opportunity create table WITH (autovacuum_enabled=false)
        # This saves CPU time and IO bandwidth on useless vacuuming of the table
        # (since we never DELETE or UPDATE the table).
        ddl = "CREATE TABLE %s.%s (" % (self.config['schema'], self.config['table'])
        for field in self.config['dtypes']:
            ddl += field + " " + self.config['dtypes'][field] + " NULL, "
        ddl += ");"
        ddl = ddl[:-4] + ")"

        self.logger.debug("[%u] Run the following sql %s" %
                          (os.getpid(), ddl))
        # we can't use
        # self.cursor.execute(ddl) and
        # self.connection.commit()
        # because when back-end is pandas self.connection means sqlalchemy engine
        # so we create another connection to postgresql
        with get_sink_connection_string(self) as conn:
            with conn.cursor() as cursor:
                cursor.execute(ddl)

        if self.table_exists():
            self.logger.info("[%u] Table's created successfully" % os.getpid())
        else:
            self.logger.info("[%u] Table hasn't created" % os.getpid())

    def get_table_metadata(self):
        """
        :return: DataFrame with table metadata
        """
        self.logger.debug("[%u] Read metadata of sink table %s" %
                          (os.getpid(), self.config['table']))
        return read_sql_query(self.query,
                              get_pandas_connection_string(self),
                              params={'schema': self.config['schema'],
                                      'table': self.config['table']})

    def check_col(self):
        """Compare col in config and in db
        :return: set of new cols
        """
        return (set(map(lambda x: x.lower(),
                        self.config['dtypes'])) -
                set(self.metadata.name.values))

    def create_column(self, new_column, dtype):
        """
            Create column in sink table
        :param new_column: str new column name
        :param dtype: str new column type
        :return: None
        """
        self.logger.debug("[%u] Ready to add column %s" %
                          (os.getpid(), new_column))
        ddl = """
            ALTER TABLE {schema}.{table}
            ADD COLUMN {col} {type}
        """
        with get_sink_connection_string(self) as conn:
            with conn.cursor() as cursor:
                cursor.execute(ddl.format(schema=self.config['schema'],
                                          table=self.config['table'],
                                          col=new_column,
                                          type=dtype))
        self.logger.debug("[%u] Column %s has been added" %
                          (os.getpid(), new_column))
