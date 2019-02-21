import logs
from config import Config
from sqlalchemy import create_engine
from pandas import read_sql


class Source:
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

        # Define column to parse as dates
        if 'date_column' in self.config:
            self.column_to_parse_as_dates = [date_column for date_column in self.config['date_column']]
        else:
            self.column_to_parse_as_dates = None
        self.logger.debug("Column to parse as dates is %s" % self.column_to_parse_as_dates)

    def engine(self):
        self.logger.info("Ready to connect %s:%s" % (self.config['host'], self.config['port']))
        source_cfg = (
            self.config['type'],
            self.config['user'],
            self.config['psw'],
            self.config['host'],
            self.config['port'],
            self.config['dbname']
        )
        try:
            self.logger.debug("Create engine")
            engine = create_engine('%s://%s:%s@%s:%s/%s' % source_cfg)
            self.logger.debug(engine)
            return engine
        except Exception as e:
            self.logger.error("Couldn't connect to source %s" % e)

    def pull_data(self):
        try:
            self.logger.info("Start load data")
            data = read_sql(self.sql, self.engine(), parse_dates=self.column_to_parse_as_dates)
            self.logger.info("Created dataframe of size %s x %s" % (data.shape[0], data.shape[1]))
            return data
        except:
            self.logger.exception("Load data from DB failed")
            raise SystemExit
