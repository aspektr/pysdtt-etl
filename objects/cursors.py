import psycopg2
import psycopg2.extras


def get_cursor(object):
    if object.config['type'] == 'mssql+pymssql':
        cursor = object.connection.cursor(as_dict=True)
        return cursor
    elif object.config['type'] == 'postgresql+psycopg2':
        cursor = object.connection.cursor(name='result_to_dict', cursor_factory=psycopg2.extras.RealDictCursor)
        return cursor
    else:
        object.logger.error("Type db is not supported")