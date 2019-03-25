import psycopg2
import psycopg2.errorcodes
import json
import os
import time
from objects.prototypes import SinkPrototype
from bson import json_util
from sortedcontainers import SortedDict


class Injector(SinkPrototype):
    def __init__(self, sink_name):
        SinkPrototype.__init__(self, sink_name)

        self.cursor = self.connection.cursor()

        if 'row-by-row' in self.args:
            self.prepare_statement = self._create_prepare_statement()
            self.insert_statement = self._create_insert_statement()
            self.cursor.execute(self.prepare_statement)

    def _create_prepare_statement(self):
        """
        :return: str prepare statement for postgresql, something like that
        PREPARE insert_data AS INSERT INTO data(github_id, type, public, created_at, actor, repo, org, payload)
                VALUES($1, $2, $3, $4, $5, $6, $7, $8);
        """
        table = self.config['schema'] + '.' + self.config['table']
        prepare_statement = "PREPARE insert_data( "

        # prepare data types str
        for column_name in self.config['dtypes']:
            prepare_statement += '%s, ' % self.config['dtypes'][column_name]
        prepare_statement = prepare_statement[:-2] + ")"
        prepare_statement += " AS INSERT INTO " + table + "("

        # prepare columns name str
        for column_name in self.config['dtypes']:
            prepare_statement += '%s, ' % column_name
        prepare_statement = prepare_statement[:-2] + ") VALUES ("

        # prepare template str for values like $1, $2, ...
        for i in range(1, len(self.config['dtypes']) + 1):
            prepare_statement += '$%s, ' % i
        prepare_statement = prepare_statement[:-2] + ');'
        self.logger.debug("[%u] Prepare statement - %s" % (os.getpid(), prepare_statement))

        return prepare_statement

    def _create_insert_statement(self):
        """
        :return: str insert statement for postgresql, something like that
        EXECUTE insert_data(%(id)s, %(type)s, %(public)s, %(created_at)s, %(actor)s, %(repo)s, %(org)s, %(payload)s);
        """
        insert_statement = """EXECUTE insert_data ("""
        for column_name in self.config['dtypes']:
            insert_statement += """%%(%s)s::%s, """ % (column_name, self.config['dtypes'][column_name])
        insert_statement = insert_statement[:-2] + ');'
        self.logger.debug("""Insert statement - %s""" % insert_statement)
        return insert_statement

    def _error_handling(self, e):
        self.logger.exception("Error while ingesting %s" % str(e))
        self.logger.exception("Error type " % psycopg2.errorcodes.lookup(e.pgcode[:2]))
        self.logger.exception("Error type " % psycopg2.errorcodes.lookup(e.pgcode))
        self.connection.rollback()
        self.cursor.execute("""DEALLOCATE PREPARE insert_data""")
        raise SystemExit

    def _wrap_arrays_and_nulls(self, row):
        """
            Checks skipped columns in row and adds null value if column is skipped,
            wraps jsonb and jsonb[] so that row can be inserted into postgres
        :param row: dict to handle
        :return: wrapped row
        """
        for column_name in self.config['dtypes']:
            # mongodb column can be omitted
            if column_name not in row:
                self.logger.debug("[%u] Column %s is being skipped" %
                                    (os.getpid(), column_name))
                row[column_name] = None if '[]' not in self.config['dtypes'][column_name] else []
            if 'jsonb[]' in self.config['dtypes'][column_name]:
                self.logger.debug("column_name: %s type: jsonb[] \n str before json.dumps: %s" %
                                  (column_name, row[column_name]))
                row[column_name] = [json.dumps(jdoc, default=json_util.default) for jdoc in row[column_name]]
                self.logger.debug("jsonb[] after json.dumps: %s" % row[column_name])
            elif 'jsonb' in self.config['dtypes'][column_name]:
                self.logger.debug("jsonb before json.dumps: %s" % row[column_name])
                row[column_name] = json.dumps(row[column_name], default=json_util.default)
                self.logger.debug("jsonb after json.dumps: %s" % row[column_name])
        return SortedDict(row)

    def ingest_from(self, producer):

        global_start_time = time.time()

        def insert(object, producer, local_start_time=time.time(), rows=0):
            for payload in producer.generate_row():
                for row in payload:
                    rows += 1
                    row = object._wrap_arrays_and_nulls(row)
                    object.logger.debug("[%u] Row for ingesting: %s" % (os.getpid(), row))
                    object.cursor.execute(self.insert_statement, row)
                    if rows == producer.config['cursor_size']:
                        took = time.time() - local_start_time
                        object.logger.info("[%u] loaded %d rows in %.2f seconds, %.2f rows/sec" %
                                           (os.getpid(), rows, took, rows/took))
                        object.connection.commit()
                        rows = 0
                        local_start_time = time.time()
            object.connection.commit()
            producer.stop_generate_row()
            total_time = time.time() - global_start_time
            object.logger.info("[%u] Total time: %s" % (os.getpid(), total_time))

        try:
            insert(self, producer)
        except Exception as e:
            self._error_handling(e)

        self.logger.info("[%u] Ingestion is done successfully" % os.getpid())

    def ingest_row(self, payload):

        def get_cols_and_vals(object):
            dtypes = SortedDict(object.config['dtypes'])
            columns = ", ".join(list(dtypes))
            values_str = ""
            for column_name in dtypes:
                values_str += """%s::""" + """%s, """ % dtypes[column_name]
            values_str = values_str[:-2]
            # TODO legacy code below is exist while you don't check how the execute method works with the renewed func
            # vals_str_list = ["%s"] * len(row_example)
            # values_str = ", ".join(vals_str_list)
            object.logger.debug("[%u] Columns for ingesting: %s" % (os.getpid(), columns))
            object.logger.debug("[%u] Template string: %s" % (os.getpid(), values_str))
            return columns, values_str

        def mogrify_execute(object, payload):
            # ingestion with mogrify
            start_local_time = time.time()
            vals = []
            cols, vals_str = get_cols_and_vals(object)

            for num_rows, inp_row in enumerate(payload):
                row = object._wrap_arrays_and_nulls(inp_row)
                vals += [[row[x] for x in row]]

            # TODO some types can can cast 2 times, for instance '2018-10-05T16:46:59'::timestamp::timestamp'
            args_str = ','.join(object.cursor.mogrify("({template})".format(template=vals_str), row).decode('utf8') for row in vals)
            table = object.config['schema'] + '.' + object.config['table']
            object.logger.debug("Mogrify values: %s" % args_str)
            object.cursor.execute("INSERT INTO {table} ({cols}) VALUES".format(table=table, cols=cols) + args_str)

            # TODO repeated piece of code is detected
            took_local = time.time() - start_local_time
            object.logger.info("[%u] cursor execute %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took_local, num_rows/took_local))

            start_local_time = time.time()
            object.connection.commit()
            # TODO repeated piece of code is detected
            took_local = time.time() - start_local_time
            object.logger.info("[%u] commit %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took_local, num_rows/took_local))
            return num_rows

        def execute(object, payload):
            # TODO add mongodb support
            # ingestion with execute
            start_local_time = time.time()
            cols, vals_str = get_cols_and_vals(self, payload)
            table = object.config['schema'] + '.' + object.config['table']

            for num_rows, inp_row in enumerate(payload):
                # TODO wrap x with _wrap_arrays_and_nulls func
                vals = [inp_row[x] for x in cols.split(', ')]
                object.logger.debug("INSERT INTO {table} ({cols}) VALUES ({vals_str})".format(
                    table=table, cols=cols, vals_str=vals_str))
                object.cursor.execute("INSERT INTO {table} ({cols}) VALUES ({vals_str})".format(
                    table=table, cols=cols, vals_str=vals_str), vals)

            took_local = time.time() - start_local_time
            object.logger.info("[%u] cursor execute %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took_local, num_rows/took_local))
            return num_rows

        start_time = time.time()
        self.logger.info("[%u] Start ingesting..." % os.getpid())
        try:
            num_rows = mogrify_execute(self, payload)
            # TODO add command prompt parameter to run execute func
            # num_rows = execute(self, payload)
        except Exception as e:
            self._error_handling(e)

        took = time.time() - start_time
        self.logger.info("[%u] loaded %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took, num_rows/took))
