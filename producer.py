import logs
from config import Config
import pymssql
import psycopg2
import psycopg2.extras
import os
import time


class Producer:
    def __init__(self, source_name):
        self.source_name = source_name

        # prepare logger
        logs.setup()
        self.logger = logs.logging.getLogger(__name__)

        # Read config
        self.config = Config({'source_name': source_name}).load()
        self.logger.info("Config for %s source is loaded" % self.source_name)
        self.logger.debug("%s config is %s" % (self.source_name, self.config))

        # Read file containing sql query
        with open(self.config['file'], 'r', encoding='utf8') as sqlfile:
            sql_query = sqlfile.read().replace('\n', ' ').replace('\t', ' ')
        self.sql = sql_query
        self.logger.info("Loading SQL query from %s" % self.config['file'])
        self.logger.debug("SQL query is %s" % self.sql)

        #
        try:
            self.connection = self.engine()
            self.cursor = self.get_cursor()
            self.cursor.execute(self.sql)
        except:
            self.logger.exception("Load data from DB failed")
            raise SystemExit

    def engine(self):
        self.logger.info("Ready to connect %s:%s" % (self.config['host'], self.config['port']))

        # specify the connection string depending on db type
        if self.config['type'] == 'mssql+pymssql':
            conn = pymssql.connect(server=self.config['host'],
                                   host=self.config['host'] + ':' + str(self.config['port']),
                                   port=self.config['port'],
                                   user=self.config['user'],
                                   password=self.config['psw'],
                                   database=self.config['dbname'],
                                   as_dict=True)
        elif self.config['type'] == 'postgresql+psycopg2':
            conn = psycopg2.connect(dbname=self.config['dbname'],
                                    user=self.config['user'],
                                    password=self.config['psw'],
                                    host=self.config['host'],
                                    port=self.config['port'])
        else:
            self.logger.error("Specify the correct type db in config file")
        return conn

    def generate_row(self):
        if self.config['type'] == 'mssql+pymssql':
            # TODO mssql and postgressql can use the same code
            # return self.cursor.__iter__()
            # generate chunks
            list_rows = []
            start_time = time.time()
            for i, row in enumerate(self.cursor):
                list_rows.append(row)
                if (i+1) % 700 == 0:  # TODO turn into config parameter "chunk_size"
                    yield list_rows
                    took = time.time() - start_time
                    self.logger.info("[%u] got %d rows in %f seconds, %f rows/sec" % (
                        os.getpid(), len(list_rows), took, len(list_rows)/took))
                    list_rows = []
            yield list_rows
        if self.config['type'] == 'postgresql+psycopg2':
            # return [row for row in self.cursor]
            # generate chunks
            list_rows = []
            start_time = time.time()
            for i, row in enumerate(self.cursor):
                list_rows.append(row)
                if (i+1) % 5000 == 0:  # TODO turn into config parameter "chunk_size"
                    yield list_rows
                    took = time.time() - start_time
                    self.logger.info("[%u] got %d rows in %f seconds, %f rows/sec" % (
                                        os.getpid(), len(list_rows), took, 1000/took))
                    list_rows = []
            if len(list_rows) != 0:
                self.logger.info("[%u] got %d rows in %f seconds, %f rows/sec" % (
                    os.getpid(), len(list_rows), took, len(list_rows)/took))
                yield list_rows

    def stop_generate_row(self):
        self.cursor.close()
        self.connection.close()

    def get_cursor(self):
        if self.config['type'] == 'mssql+pymssql':
            cursor = self.connection.cursor(as_dict=True)
            return cursor
        elif self.config['type'] == 'postgresql+psycopg2':
            cursor = self.connection.cursor(name='result_to_dict', cursor_factory=psycopg2.extras.RealDictCursor)
            return cursor
        else:
            self.logger.error("Type db is not supported")
