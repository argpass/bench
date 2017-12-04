#!coding: utf-8
import os
import sys
import time
import logging
from .pool import PyMySQLPool, SharedConnection, MySQLConFactory

logger = logging.getLogger(__name__)


class OperationalError(Exception):
    def __init__(self, raw_error):
        self._raw_error = raw_error


class MultipleRowsError(Exception):
    """"""


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __init__(self, col_names, data_tuple):
        super(Row, self).__init__(zip(col_names, data_tuple))
        self.__col_names__ = col_names
        self.__data__ = data_tuple

    def get_col_names(self):
        """
        Returns:
            tuple
        """
        return self.__col_names__

    def get_data_tuple(self):
        """
        Returns:
            tuple
        """
        return self.__data__

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


class Rows(object):

    class IterOnClosedRows(Exception):
        pass

    def __init__(self, cursor, con, database):
        """
        Args:
            cursor:
            con(DriverConnection):
            database(Database):
        """
        self._cursor = cursor
        self._connection = con
        self._db = database
        self._free = getattr(self._db, "_free")
        self._closed = False
        self._gen = None

    def close(self, err):
        if self._closed:
            return
        try:
            self._cursor.close()
        finally:
            self._free(self._connection, err)
            self._closed = True

    @property
    def is_closed(self):
        return self._closed

    def _iter(self):
        err = None
        try:
            column_names = [d[0] for d in self._cursor.description]
            for row in self._cursor:
                if self.is_closed:
                    raise self.IterOnClosedRows(u"iter on a closed "
                                                u"`Rows` is denied")
                yield Row(column_names, row)
        except Exception as e:
            err = e
            raise e
        finally:
            self.close(err)

    def __iter__(self):
        return self

    def __next__(self):
        if self._gen is None:
            self._gen = self._iter()
        return next(self._gen)


class Database(object):
    """Database wrapper, all operation is on a connection cursor
    all connections managed in a connection pool
    """
    OperationalError = None

    def __init__(self, pool):
        self._pool = pool

    @classmethod
    def log(cls, sql, args, kwargs, cost=None, err=None):
        if cost is None:
            cost_ms = 0
        else:
            cost_ms = cost * 1000
        if logger.isEnabledFor(logging.DEBUG):
            f = getattr(sys, "_getframe")(2)
            name = "__main__"
            file_no = ""
            while True:
                if f.f_back:
                    f_code = f.f_back.f_code
                    file_name = os.path.normcase(f_code.co_filename)
                    file_no = f.f_back.f_lineno
                    if file_name == __file__:
                        f = f.f_back
                        continue
                    name = os.path.basename(file_name).replace(".py", "")
                    break
                else:
                    break
            caller_info = u"%s:%s" % (name, file_no)
            logger.debug(u"[%s][SQL]%s, --args:%s, kwargs:%s, cost:%sms, err:%s",
                         caller_info,
                         sql,
                         repr(args), repr(kwargs), cost_ms, err)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        con = self._allocate()
        cursor = con.cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return Rows(cursor, con, self)
        except Exception as e:
            cursor.close()
            self._free(con, e)
            raise e

    def query(self, query, *parameters, **kwparameters):
        return list(self.iter(query, *parameters, **kwparameters))

    def get(self, query, *parameters, **kwparameters):
        """Returns the (singular) row returned by the given query.

        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = list(self.iter(query, *parameters, **kwparameters))
        if not rows:
            return None
        elif len(rows) > 1:
            raise MultipleRowsError(u"Multiple rows returned "
                                    u"for Database.get() query")
        else:
            return rows[0]

    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        except Exception as e:
            err = e
            raise e
        finally:
            cursor.close()
            self._free(con, err)

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        con = self._allocate()
        cursor = con.cursor()
        err = None
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        except Exception as e:
            err = e
            raise e
        finally:
            cursor.close()
            self._free(con, err)

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the lastrowid from the query.
        """
        con = self._allocate()
        cursor = con.cursor()
        err = None
        t_start = time.time()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        except Exception as e:
            err = e
            raise e
        finally:
            self.log(query, parameters, None, time.time() - t_start, err)
            cursor.close()
            self._free(con, err)

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.

        We return the rowcount from the query.
        """
        con = self._allocate()
        cursor = con.cursor()
        err = None
        t_start = time.time()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        except Exception as e:
            err = e
            raise e
        finally:
            self.log(query, parameters, None, time.time() - t_start, err)
            cursor.close()
            self._free(con, err)

    update = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _execute(self, cursor, query, parameters, kwparameters):
        t_start = time.time()
        err = None
        try:
            return cursor.execute(query, kwparameters or parameters)
        except Exception as e:
            # self.OperationalError is driver specific, so i
            # wrap self.OperationalError as `OperationalError`,
            # then all OperationalError is the same
            err = e
            raise OperationalError(e)
        finally:
            t_end = time.time()
            self.log(query, parameters, kwparameters,
                     cost=t_end - t_start, err=err)

    def transaction(self):
        """
        Returns:
            Transaction
        """
        con = self._pool.allocate()
        return Transaction(con, self._pool)

    def begin(self):
        """
        Returns:
            Transaction
        """
        return self.transaction()

    def _allocate(self):
        return self._pool.allocate()

    def _free(self, con, err=None):
        return self._pool.free(con, err)

    def close(self):
        pass


class Transaction(Database):
    def __init__(self, con, pool, is_inner_transaction=False):
        """
        Args:
            con(SharedConnection):
            pool(PyMySQLPool):
        """
        super(Transaction, self).__init__(pool)
        self._con = con
        self._err = None
        self._is_inner_transaction = is_inner_transaction
        self._closed = False

    def __del__(self):
        """try to close myself if i'm dead
        """
        try:
            self.close()
        except Exception:
            pass

    def transaction(self):
        con = self._con
        return Transaction(con, self._pool, is_inner_transaction=True)

    def _allocate(self):
        """Just to return the connection i hold

        Returns:
            SharedConnection
        """
        return self._con

    def _free(self, con, err=None):
        """no operation, free con in the `close` method
        Args:
            con(SharedConnection)
        """
        pass

    def commit(self):
        """commit immediately if i'm not an inner transaction
        Returns:
            bool
        """
        if self._is_inner_transaction:
            return False
        self._con.commit()
        return True

    def rollback(self):
        return self._con.rollback()

    def close(self):
        if self._closed:
            return
        self._closed = True
        # An inner transaction shouldn't free the shared connection
        if not self._is_inner_transaction:
            self._pool.free(self._con)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()


def new(user, password, database, maximum=1, idle_maximum=1,
        host="localhost", port=3306, charset="utf8"):
    """
    Args:
        user(str)
        password(str)
        database(str)
        maximum(int)
        idle_maximum(int)
        host(str)
        port(int)
        charset(str)

    Returns:
        Database
    """

    factory = MySQLConFactory(host, user, password, database, port, charset)
    pool = PyMySQLPool(maximum, idle_maximum, factory)
    return Database(pool)

