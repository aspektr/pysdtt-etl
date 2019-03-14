import os
from sqlalchemy.dialects.postgresql import \
    ARRAY, BIGINT, BIT, BOOLEAN, BYTEA, CHAR, CIDR, DATE, \
    DOUBLE_PRECISION, ENUM, FLOAT, HSTORE, INET, INTEGER, \
    INTERVAL, JSON, JSONB, MACADDR, MONEY, NUMERIC, OID, REAL, SMALLINT, TEXT, \
    TIME, TIMESTAMP, UUID, VARCHAR
from objects.prototypes import SinkPrototype


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


class Sink(SinkPrototype):
    #def __init__(self, sink_name):
    #    Prototype.__init__(self, sink_name, kind='sink')

    def type_mapping(self):
        dtypes = {}
        self.logger.info("[%u] Starting data types mapping" % os.getpid())

        def map_types_without_length(dtypes):
            self.logger.debug("[%u] The field %s has type %s and doesn't contain a length" %
                              (os.getpid(), field, self.config['dtypes'][field]))
            dtypes[field] = db_dtypes[self.config['dtypes'][field]]
            self.logger.debug("[%u] The field %s is mapped to a %s" %
                              (os.getpid(), field, db_dtypes[self.config['dtypes'][field]]))

        def map_types_having_length(dtypes):
            self.logger.debug("[%u] The field %s has type %s. It's necceary to isolate the number in ()" %
                              (os.getpid(), field, self.config['dtypes'][field]))
            # extract length from type, for instance: varchar(128) => 128
            length = int(''.join([char for char in self.config['dtypes'][field] if char.isdigit()]))
            # extract type
            f_type = self.config['dtypes'][field]
            f_type = f_type[: f_type.find('(')]
            dtypes[field] = db_dtypes[f_type](length)
            self.logger.debug("[%u] The field %s is mapped to a %s" %
                              (os.getpid(), field, db_dtypes[f_type](length)))

        # TODO test types-arrays like int[], json[], jsonb[]
        for field in self.config['dtypes']:
            if '(' not in self.config['dtypes'][field]:
                map_types_without_length(dtypes)
            else:
                map_types_having_length(dtypes)

        self.logger.debug("Dtypes are %s" % dtypes)
        return dtypes

    def push_data(self, payload):
        self.logger.info("[%u] Start ingesting...." % os.getpid())
        dtypes = self.type_mapping()
        schema = self.config['schema'] if self.config['schema'] else None

        def insert_data(payload):
            payload.to_sql(self.config['table'],
                           self.connection,
                           schema=schema,
                           if_exists=self.config['if_exists'],
                           dtype=dtypes,
                           method=self.config['method'],
                           index=False)
        try:
            insert_data(payload)
        # TODO bare except!!!
        except:
            self.logger.exception("[%u] Load data to DB failed" % os.getpid())
            raise SystemExit
        self.logger.info("[%u] Task is done successfully" % os.getpid())
