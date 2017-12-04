#!coding: utf-8
import logging
import os

import time

from pending.log import enable_pretty_logging, patch_logging_for_tracking


SHOW_SQL = False


class SQLError(Exception):
    def __init__(self, sql, args, err):
        self.sql = sql
        self.args = args
        self.err = err

    def __str__(self):
        sql = repr(self.sql)
        args = repr(self.args)
        return "SQLErr:{sql},--args:{args}, err:{err}".format(
            sql=sql, args=args, err=str(self.err))

    def __unicode__(self):
        return self.__str__()


def execute(self, query, args=None):
    """Execute a query

    :param str query: Query to execute.

    :param args: parameters used with query. (optional)
    :type args: tuple, list or dict

    :return: Number of affected rows
    :rtype: int

    If args is a list or tuple, %s can be used as a placeholder in the query.
    If args is a dict, %(name)s can be used as a placeholder in the query.
    """
    global SHOW_SQL
    while self.nextset():
        pass

    try:
        begin = time.time()
        raw_query = query
        query = self.mogrify(query, args)
        result = self._query(query)
        self._executed = query
        cost = (time.time() - begin) * 1000
        if SHOW_SQL:
            logging.info("[SQL]%s, --args:%s, cost %.3f ms",
                         repr(raw_query), repr(args), cost)
        return result

    except Exception as e:
        raise SQLError(query, args, e)


def setup():
    project_dir = os.path.dirname(os.path.join(os.path.dirname(__file__),
                                               "../../"))
    os.environ.setdefault("CONFIG_FILE", os.path.join(
        project_dir, "conf/settings.json"))


def configure(log_level="INFO", show_sql=False):
    # configure logging
    patch_logging_for_tracking()
    enable_pretty_logging(log_level, force=True)
    # patch pymysql
    global SHOW_SQL
    SHOW_SQL = show_sql
    from pymysql.cursors import Cursor
    Cursor.execute = execute
