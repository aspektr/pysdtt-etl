import pymssql
import psycopg2
from sqlalchemy import create_engine
import os


#  TODO add error handling
def engine(object):
    object.logger.info("[%u] Ready to connect %s:%s" %
                       (os.getpid(), object.config['host'], object.config['port']))

    # pandas - if all_data specified then we use pandas back-end both for source and sink
    if 'all_data' in object.args:
        return get_pandas_connection_string(object)
    elif object.type == 'source':
        return get_source_connection_string(object)
    elif object.type == 'sink':
        return get_sink_connection_string(object)
    else:
        object.logger.error("Specify the correct type db in config file or correct arguments")


def get_pandas_connection_string(object):
    cfg = (
        object.config['type'],
        object.config['user'],
        object.config['psw'],
        object.config['host'],
        object.config['port'],
        object.config['dbname']
    )
    object.logger.debug("[%u] Create engine" % os.getpid())
    conn = create_engine('%s://%s:%s@%s:%s/%s' % cfg)
    object.logger.debug(conn)
    return conn


def get_source_connection_string(object):
    # specify the connection string depending on db type
    if object.config['type'] == 'mssql+pymssql':
        conn = get_pymssql_connection(object)
        return conn
    elif object.config['type'] == 'postgresql+psycopg2':
        conn = get_psycopg_connection(object)
        return conn


def get_sink_connection_string(object):
    return get_psycopg_connection(object)


def get_pymssql_connection(object):
    return pymssql.connect(server=object.config['host'],
                           host=object.config['host'] + ':' + str(object.config['port']),
                           port=object.config['port'],
                           user=object.config['user'],
                           password=object.config['psw'],
                           database=object.config['dbname'],
                           as_dict=True)


def get_psycopg_connection(object):
    return psycopg2.connect(dbname=object.config['dbname'],
                            user=object.config['user'],
                            password=object.config['psw'],
                            host=object.config['host'],
                            port=object.config['port'])
