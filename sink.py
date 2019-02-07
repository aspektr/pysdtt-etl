import logs
from config import Config
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
    INTERVAL, JSON, JSONB, MACADDR, MONEY, NUMERIC, OID, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR


db_dtypes = {
    'array': ARRAY,
    'bigint': BIGINT,
    'bit': BIT,
    'boolean': BOOLEAN,
    'bytea': BYTEA,
    'char': CHAR,
    'cidr': CIDR,
    'date': DATE,
    'double': DOUBLE_PRECISION,
    'enum': ENUM,
    'float': FLOAT,
    'hstore': HSTORE,
    'inet': INET,
    'integer': INTEGER,
    'int4': INTEGER,
    'interval': INTERVAL,
    'json': JSON,
    'jsonb': JSONB,
    'macaddr': MACADDR,
    'money': MONEY,
    'numeric': NUMERIC,
    'oid': OID,
    'real': REAL,
    'smallint': SMALLINT,
    'int2': SMALLINT,
    'text': TEXT,
    'time': TIME,
    'timestamp': TIMESTAMP,
    'uuid': UUID,
    'varchar': VARCHAR
}


class Sink:
    def __init__(self, sink_name):
        self.sink_name = sink_name

        # Prepare logger
        logs.setup()
        self.logger = logs.logging.getLogger(__name__)

        # Read config
        self.config = Config({'sink_name': self.sink_name}).load()
        self.logger.info("Config for %s sink is loaded" % self.sink_name)
        self.logger.debug("%s config is %s" % (self.sink_name, self.config))

    def engine(self):
        self.logger.info("Ready to ingest data into %s:%s/%s" %
                         (self.config['host'], self.config['port'], self.config['table']))
        sink_cfg = (
            self.config['type'],
            self.config['user'],
            self.config['psw'],
            self.config['host'],
            self.config['port'],
            self.config['dbname']
        )
        return create_engine('%s://%s:%s@%s:%s/%s' % sink_cfg)

    def type_mapping(self):
        dtypes = {}
        self.logger.debug("Starting data types mapping")
        for field in self.config['dtypes']:
            if '(' not in self.config['dtypes'][field]:
                self.logger.debug("The field %s has type %s and doesn't contain a length" %
                                  (field, self.config['dtypes'][field]))
                dtypes[field] = db_dtypes[self.config['dtypes'][field]]
                self.logger.debug("The field %s is mapped to a %s" %
                                  (field, db_dtypes[self.config['dtypes'][field]]))
            else:
                self.logger.debug("The field %s has type %s. It's necceary to isolate the number in ()" %
                                  (field, self.config['dtypes'][field]))
                # extract length from type, for instance: varchar(128) => 128
                length = int(''.join([char for char in self.config['dtypes'][field] if char.isdigit()]))
                # extract type
                f_type = self.config['dtypes'][field]
                f_type = f_type[: f_type.find('(')]
                dtypes[field] = db_dtypes[f_type](length)
                self.logger.debug("The field %s is mapped to a %s" %
                                  (field, db_dtypes[f_type](length)))
        self.logger.debug("Dtypes are %s" % dtypes)
        return dtypes

    def push_data(self, payload):
        dtypes = self.type_mapping()
        schema = self.config['schema'] if self.config['schema'] else None
        try:
            payload.to_sql(self.config['table'],
                           self.engine(),
                           schema=schema,
                           if_exists=self.config['if_exists'],
                           dtype=dtypes,
                           method=self.config['method'])
        except:
            self.logger.exception("Load data to DB failed")
            raise SystemExit
        self.logger.info("Task is done successfully")
