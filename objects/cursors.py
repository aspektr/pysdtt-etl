import psycopg2
import psycopg2.extras


def get_cursor(object):
    if object.config['type'] == 'mssql+pymssql':
        cursor = object.connection.cursor(as_dict=True)
        return cursor
    elif object.config['type'] == 'postgresql+psycopg2':
        cursor = object.connection.cursor(name='result_to_dict', cursor_factory=psycopg2.extras.RealDictCursor)
        return cursor
    elif object.config['type'] == 'mongodb':
        if 'filter' not in object.query:
            object.query['filter'] = None
        cursor = object.connection.find(object.query['filter'], object.query['projection'])
        # gap-closure because mongo cursor hasn't execute method
        cursor.execute = lambda x: None
        return cursor
    else:
        object.logger.error("Type db is not supported")