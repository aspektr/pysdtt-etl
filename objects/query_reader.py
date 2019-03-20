import yaml


def read_query(object):
    # Read file containing query
    if object.type == 'sink':
        object.config['file'] = 'query/postgres_table_metadata.sql'
    with open(object.config['file'], 'r', encoding='utf8') as query_file:
        if object.config['type'] == 'mongodb':
            query = yaml.load(query_file)
        else:
            query = query_file.read().replace('\n', ' ').replace('\t', ' ')
    return query
