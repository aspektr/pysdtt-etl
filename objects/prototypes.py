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
        if '--conf' in self.args:
            self.config = Config({config_section_name: self.name}, self.args[2]).load()
        else:
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

        # TODO add something like that cur.execute("SET TIME ZONE 'Europe/Rome';") where timezone is param

        if not self.table_exists():
            self.create_table()
        # pandas already has handler for if_exists param, thus we need implement it only for another modes
        elif 'all_data' not in self.args:
            self._if_exists_execute()

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
        self.logger.info("[%u] Table %s doesn't exist. Creating..." %
                         (os.getpid(), self.config['table']))
        ddl = "CREATE TABLE IF NOT EXISTS %s.%s (" % (self.config['schema'], self.config['table'])
        for field in self.config['dtypes']:
            ddl += field + " " + self.config['dtypes'][field] + " NULL, "
        ddl += ");"
        ddl = ddl[:-4] + ")"

        self.logger.debug("[%u] Run the following sql %s" %
                          (os.getpid(), ddl))
        # TODO fix problem when we create table concurrently
        """
        https://www.postgresql.org/message-id/CA+TgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg@mail.gmail.com
        
        On Mon, Apr 23, 2012 at 7:49 AM, Matteo Beccati <php(at)beccati(dot)com> wrote:
        > I've tried to come up with a self-contained test case but I haven't been
        > able to replicate the error above. However the following script performs a
        > few concurrent CREATE TABLE IF NOT EXISTS statements that produce some
        > unexpected errors (using 9.1.2).
        > ERROR:  duplicate key value violates unique constraint
        > "pg_type_typname_nsp_index"



        This is normal behavior for CREATE TABLE either with or without IF NOT
        EXISTS.  CREATE TABLE does a preliminary check to see whether a name
        conflict exists.  If so, it either errors out (normally) or exits with
        a notice (in the IF NOT EXISTS case).  But there's a race condition: a
        conflicting transaction can create the table after we make that check
        and before we create it ourselves.  If this happens, then you get the
        failure you're seeing, because the btree index machinery catches the
        problem when we do the actual system catalog inserts.



        Now, this is not very user-friendly, but we have no API to allow
        inserting into a table with a "soft" error if uniqueness would be
        violated.  Had we such an API we could handle a number of situations
        more gracefully, including this one.  Since we don't, the only option
        is to let the btree machinery error out if it must.



        The bottom line is that CREATE TABLE IF NOT EXISTS doesn't pretend to
        handle concurrency issues any better than regular old CREATE TABLE,
        which is to say not very well.  You should use some other system to
        coordinate near-simultaneous creation of tables, such as perhaps doing
        pg_advisory_lock/CINE/pg_advisory_unlock.
        
        
        
        -- 
        Robert Haas
        EnterpriseDB: http://www.enterprisedb.com
        The Enterprise PostgreSQL Company
        
        """
        self.execute_ddl(ddl)
        if self.table_exists():
            self.logger.info("[%u] Table's created successfully" % os.getpid())
        else:
            self.logger.info("[%u] Table hasn't created" % os.getpid())

    def truncate_table(self):
        self.logger.info("[%u] Table %s will be truncated..." %
                         (os.getpid(), self.config['table']))
        ddl = "TRUNCATE TABLE %s.%s;" % (self.config['schema'], self.config['table'])
        self.logger.debug("[%u] Run the following sql %s" %
                          (os.getpid(), ddl))
        self.execute_ddl(ddl)

        # TODO add message
        # TODO create func table_num_rows
        # if self.table_num_rows():
        #    self.logger.info("[%u] Table's truncated successfully" % os.getpid())
        # else:
        #    self.logger.info("[%u] Table hasn't truncated" % os.getpid())

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
            ADD COLUMN IF NOT EXISTS {col} {type}
        """
        # TODO Replace by execute_ddl func and test it
        with get_sink_connection_string(self) as conn:
            with conn.cursor() as cursor:
                cursor.execute(ddl.format(schema=self.config['schema'],
                                          table=self.config['table'],
                                          col=new_column,
                                          type=dtype))
        self.logger.debug("[%u] Column %s has been added" %
                          (os.getpid(), new_column))

    def _if_exists_execute(self):
        """
        if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’
        How to behave if the table already exists.

        fail: Raise a ValueError.
        replace: Drop the table before inserting new values.
        append: Insert new values to the existing table.
        """
        try:
            if self.config['if_exists'] == 'fail':
                raise ValueError("Table '%s' already exists" % self.config['table'])
            elif self.config['if_exists'] == 'replace':
                self.truncate_table()
            elif self.config['if_exists'] == 'append':
                pass
            else:
                raise ValueError("[%u] '%s' is not valid for if_exists" %
                                 (os.getpid(), self.config['if_exists']))
        except Exception:
            self.logger.exception("[%u] Load data to DB failed" % os.getpid())
            raise SystemExit(1)

    def execute_ddl(self, ddl):
        """
            we can't use
        self.cursor.execute(ddl) and
        self.connection.commit()
        because when back-end is pandas self.connection means sqlalchemy engine
        so we create another connection to postgresql
        """
        with get_sink_connection_string(self) as conn:
            with conn.cursor() as cursor:
                cursor.execute(ddl)

