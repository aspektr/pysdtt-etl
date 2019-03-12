import os
import time
from objects.prototypes import SourcePrototype
from objects.cursors import get_cursor


class Producer(SourcePrototype):
    def __init__(self, source_name):
        SourcePrototype.__init__(self, source_name)
        try:
            self.cursor = get_cursor(self)
            self.cursor.execute(self.sql)
        # TODO bare except!!!
        except:
            self.logger.exception("Load data from DB failed")
            raise SystemExit

    def generate_row(self):

        def print_time_exec(start_time):
            took = time.time() - start_time
            self.logger.info("[%u] got %d rows in %f seconds, %f rows/sec" % (
                os.getpid(), len(list_rows), took, len(list_rows)/took))

        def set_initial_values():
            return [], time.time()

        list_rows, start_time = set_initial_values()
        for i, row in enumerate(self.cursor):
            list_rows.append(row)
            if (i+1) % self.config['cursor_size'] == 0:
                print_time_exec(start_time)
                yield list_rows
                list_rows, start_time = set_initial_values()

        print_time_exec(start_time)
        yield list_rows

    def stop_generate_row(self):
        self.cursor.close()
        self.connection.close()
