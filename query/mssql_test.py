import pymssql
import yaml
from config import Config


if __name__ == '__main__':
    print("start")
    config = Config({'source_name': 'test_source'}, path="../configs/config.yaml").load()
    conn = pymssql.connect(server=config['host'],
                          host=config['host'] + ':' + str(config['port']),
                          port=config['port'],
                          user=config['user'],
                          password=config['psw'],
                          database=config['dbname'],
                          as_dict=True)
    print("config loaded")
    with open(config['file'], 'r', encoding='utf8') as sqlfile:
        sql_query = sqlfile.read().replace('\n', ' ').replace('\t', ' ')
    print("sql loaded")
    cursor = conn.cursor(as_dict=True)
    print("cursor ready")
    cursor.execute(sql_query)
    print("cursor executed")

    for i, row in enumerate(cursor):
        print(row)
        if i == 5:
            break

    print("something")

    for i, row in enumerate(cursor):
        print(row)
        if i == 5:
            break
