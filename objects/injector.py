import psycopg2
import psycopg2.errorcodes
import os
import time
from objects.prototypes import Prototype


class Injector(Prototype):
    def __init__(self, sink_name):
        Prototype.__init__(self, sink_name, kind='sink')

        self.prepare_statement = self.create_prepare_statement()
        self.insert_statement = self.create_insert_statement()

        self.cursor = self.connection.cursor()
        if self.table_exists():
            self.cursor.execute(self.prepare_statement)
        else:
            self.logger.info("Table %s doesn't exist. Creating..." % self.config['table'])
            self.create_table()
            self.cursor.execute(self.prepare_statement)

    def create_prepare_statement(self):
        """
        :return: str prepare statement for postgresql, something like that
        PREPARE insert_data AS INSERT INTO data(github_id, type, public, created_at, actor, repo, org, payload)
                VALUES($1, $2, $3, $4, $5, $6, $7, $8);
        """
        table = self.config['schema'] + '.' + self.config['table']
        prepare_statement = "PREPARE insert_data AS INSERT INTO " + table + "("
        for field_name in self.config['dtypes']:
            prepare_statement += '%s, ' % field_name
        prepare_statement = prepare_statement[:-2] + ") VALUES ("

        for i in range(1, len(self.config['dtypes']) + 1):
            prepare_statement += '$%s, ' % i
        prepare_statement = prepare_statement[:-2] + ');'
        self.logger.info("[%u] Prepare statement - %s" % (os.getpid(), prepare_statement))

        return prepare_statement

    def create_insert_statement(self):
        """
        :return: str insert statement for postgresql, something like that
              EXECUTE insert_data(%(id)s, %(type)s, %(public)s, %(created_at)s, %(actor)s, %(repo)s, %(org)s, %(payload)s);
        """
        insert_statement = """EXECUTE insert_data ("""
        for field_name in self.config['dtypes']:
            insert_statement += """%%(%s)s, """ % field_name
        insert_statement = insert_statement[:-2] + ');'
        self.logger.info("""Insert statement - %s""" % insert_statement)
        return insert_statement

    def ingest_from(self, producer):
        local_start_time = time.time()
        global_start_time = local_start_time
        rows = 0
        try:
            for payload in producer.generate_row():
                for row in payload:
                    rows += 1
                    self.logger.debug("Row for ingesting: %s" % row)
                    self.cursor.execute(self.insert_statement, row)
                    if rows == 100:  # TODO add batch_size into config
                        took = time.time() - local_start_time
                        self.logger.info("[%u] loaded %d rows in %.2f seconds, %.2f rows/sec" %
                                         (os.getpid(), rows, took, rows/took))
                        self.connection.commit()
                        rows = 0
                        local_start_time = time.time()
            self.connection.commit()
            producer.stop_generate_row()
            total_time = time.time() - global_start_time
            self.logger.info("Total time: %s" % total_time)
        except Exception as e:
            self.logger.exception("Error while ingesting %s" % str(e))
            self.logger.exception("Error type " % psycopg2.errorcodes.lookup(e.pgcode[:2]))
            self.logger.exception("Error type " % psycopg2.errorcodes.lookup(e.pgcode))
            self.connection.rollback()
            self.cursor.execute("""DEALLOCATE PREPARE insert_data""")
            producer.stop_generate_row()
            raise SystemExit

        self.logger.info("Ingestion is done successfully")

    def table_exists(self):
        self.logger.info("Check the table %s.%s for existing" % (self.config['schema'], self.config['table']) )
        self.cursor.execute("select * from information_schema.tables where table_name='%s'" % (self.config['table']))
        self.logger.debug("Exist? %s" % self.cursor.rowcount)
        if self.cursor.rowcount:
            self.logger.info("The table '%s' exists" % self.config['table'])
        return self.cursor.rowcount

    def create_table(self):
        # TODO add an opportunity (via config) create UNLOGGED TABLE
        # TODO add an opportunity create tabele WITH (autovacuum_enabled=false)
        # This saves CPU time and IO bandwidth on useless vacuuming of the table
        # (since we never DELETE or UPDATE the table).
        ddl = "CREATE TABLE %s.%s (" % (self.config['schema'], self.config['table'])
        for field in self.config['dtypes']:
            ddl += field + " " + self.config['dtypes'][field] + " NULL, "
        ddl += ");"
        ddl = ddl[:-4] + ")"

        self.logger.debug("Run the following sql %s" % ddl)

        self.cursor.execute(ddl)
        self.connection.commit()

        if self.table_exists():
            self.logger.info("Table's created successfully")
        else:
            self.logger.info("Table hasn't created")

    def ingest_row(self, payload):
        try:
            start_time = time.time()
            self.logger.info("[%u] start loading" % os.getpid())

            # ingestion with prepare statement
            #start_local_time = time.time()
            #for num_rows, row in enumerate(payload):
            #    self.logger.debug("Row for ingesting: %s" % row)
            #    self.cursor.execute(self.insert_statement, row)
            #took_local = time.time() - start_local_time
            #self.logger.info("[%u] cursor execute %d rows in %f seconds, %f rows/sec" % (
            #    os.getpid(), num_rows+1, took_local, num_rows/took_local))

            row_example = payload[0]
            cols = ", ".join(list(row_example.keys()))
            vals_str_list = ["%s"] * len(row_example)
            vals_str = ", ".join(vals_str_list)
            self.logger.info("Columns for ingesting: %s" % cols)

            # ingestion with execute
            """start_local_time = time.time()
            for num_rows, inp_row in enumerate(payload):
                vals = [inp_row[x] for x in cols.split(', ')]
                # TODO replace temp_table with parameter fro, config file
                self.logger.debug("INSERT INTO upload.temp_table ({cols}) VALUES ({vals_str})".format(
                    cols=cols, vals_str=vals_str))
                self.cursor.execute("INSERT INTO upload.temp_table ({cols}) VALUES ({vals_str})".format(
                        cols=cols, vals_str=vals_str), vals)
            took_local = time.time() - start_local_time
            self.logger.info("[%u] cursor execute %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took_local, num_rows/took_local))
            """


            # ingestion with mogrify
            start_local_time = time.time()
            vals = []
            for num_rows, inp_row in enumerate(payload):
                vals += [[inp_row[x] for x in inp_row]]
            args_str = ','.join(self.cursor.mogrify("({template})".format(template=vals_str), row).decode('utf8') for row in vals)
            # TODO replace temp_table. Use args from prompt
            self.cursor.execute("INSERT INTO upload.temp_table ({cols}) VALUES".format(cols=cols) + args_str)
            took_local = time.time() - start_local_time
            self.logger.info("[%u] cursor execute %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took_local, num_rows/took_local))            


            start_local_time = time.time()
            self.connection.commit()
            took_local = time.time() - start_local_time
            self.logger.info("[%u] commit %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took_local, num_rows/took_local))
        except Exception as e:
            self.logger.exception("Error while ingesting %s" % str(e))
            self.logger.exception("Error type " % psycopg2.errorcodes.lookup(e.pgcode[:2]))
            self.logger.exception("Error type " % psycopg2.errorcodes.lookup(e.pgcode))
            self.connection.rollback()
            self.cursor.execute("""DEALLOCATE PREPARE insert_data""")
            producer.stop_generate_row() # TODO close connection
            raise SystemExit
        # TODO number of rows need to pick up from config file
        took = time.time() - start_time
        self.logger.info("[%u] loaded %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), num_rows+1, took, num_rows/took))
