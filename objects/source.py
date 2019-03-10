
from pandas import read_sql
from objects.prototypes import SourcePrototype
import os


class Source(SourcePrototype):
    def pull_data(self):
        try:
            self.logger.info("[%u] Start load data" % os.getpid())
            data = read_sql(self.sql, self.connection, parse_dates=self.column_to_parse_as_dates)
            self.logger.info("[%u] Created dataframe of size %s x %s" %
                             (os.getpid(), data.shape[0], data.shape[1]))
            return data
        except:
            self.logger.exception("Load data from DB failed")
            raise SystemExit
