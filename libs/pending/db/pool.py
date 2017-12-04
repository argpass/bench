#!coding: utf-8
import time
import os
import logging
from multiprocessing import reduction
from pending.sync import ThreadSharedPool, ResourceFactory
from pymysql.connections import Connection, Cursor


_logger = logging.getLogger(__name__)
CON_CHECK_INTERVAL_S = 2 * 3600


class SharedConnection(Connection):
    cursorclass = Cursor
    __id__ = None
    __last_check_t__ = None


class ProcessSharedConnection(SharedConnection):
    """ProcessSharedConnection can be passed
    from a process to another through `Queue`
    """
    cursorclass = Cursor
    __id__ = None

    def __getstate__(self):
        """
        Here are properties should be cloned when passing me to another process
        Returns:
            dict
        """
        server_state = {
            "server_capabilities": self.server_capabilities,
            "server_charset": self.server_charset,
            "server_language": self.server_language,
            "server_status": self.server_status,
            "server_thread_id": self.server_thread_id,
            "server_version": self.server_version,
        }
        con_args = {
            "ssl": self.ssl,
            "salt": self.salt,
            "host": self.host,
            "host_info": self.host_info,
            "password": self.password,
            "db": self.db,
            "port": self.port,
            "user": self.user,
            "connect_timeout": self.connect_timeout,
            "protocol_version": self.protocol_version,
            "charset": self.charset,
            "encoding": self.encoding,
        }
        state = {
            "sql_mode": self.sql_mode,
            "use_unicode": self.use_unicode,
            "decoders": self.decoders,
            # "encoders": self.encoders,  # todo: encoders can't be passed
            "client_flag": self.client_flag,
            "init_command": self.init_command,
            "autocommit_mode": self.autocommit_mode,
            "_result": self._result,
            "_write_timeout": self._write_timeout,
            "_read_timeout": self._read_timeout,
            "__id__": self.__id__,
        }
        state.update(server_state)
        state.update(con_args)
        state["__fd__"] = os.dup(self._sock.fileno())

        state["_sock"] = reduction.reduce_socket(self._sock)
        packed = dict()
        for k, v in state.items():
            if v is None:
                v = 'None'
            packed[k] = v
        return packed

    def __setstate__(self, packed):
        state = dict()
        for k, v in packed.items():
            if v == 'None':
                v = None
            state[k] = v

        reduced_sock = state.pop('_sock')
        fd = state.pop("__fd__")
        for k, v in state.items():
            setattr(self, k, v)

        # hook some methods to be empty after `pickle.loads` before connect
        self._get_server_information = lambda: None
        self._request_authentication = lambda: None

        # never to close the connection
        def close():
            raise ValueError(u"never to close the connection")

        self.close = close

        rebuild, (hd, family, tp, proto) = reduced_sock
        sock = rebuild(hd, family, tp, proto)
        self.connect(sock)


class MySQLConFactory(ResourceFactory):

    def __init__(self, host, user, password, database, port=3306,
                 charset='utf8'):
        """
        Args:
            host(str)
            user(str)
            password(str)
            database(str)
            port(int)
            charset(str)
        """

        con_args = dict(host=host, user=user, password=password,
                        database=database, port=port, charset=charset,
                        autocommit=True)
        self._con_args = con_args
        self._connections = dict()

    def is_valid(self, res):
        # There are conditions need to do check
        con, err = res
        if err or time.time() - con.__last_check_t__ > CON_CHECK_INTERVAL_S \
                or getattr(con, "_sock", None) is None:
            # do check once
            try:
                ok = con.ping(False)
                con.__last_check_t__ = time.time()
                return ok is not None and ok is not False
            except Exception as e:
                return False
        return True

    def free(self, res):
        con, err = res
        _logger.debug(
            "To free: %s, remains:%s" % (con.__id__, self._connections.keys()))
        try:
            raw_con = self._connections.pop(con.__id__)
        except KeyError:
            return False
        else:
            raw_con.close()
            return True

    def _create(self):
        _logger.debug("To create a new connection")
        con = SharedConnection(**self._con_args)
        con.connect()
        # attach internal properties for the connection factory
        con.__id__ = id(con)
        con.__last_check_t__ = time.time()
        self._connections[con.__id__] = con
        return con

    def new(self):
        con = self._create()
        return con, None


class PyMySQLPool(ThreadSharedPool):

    def allocate(self, timeout=None):
        """
        Args:
            timeout:

        Returns:
            SharedConnection
        """
        con, _ = super(PyMySQLPool, self).allocate(timeout)
        return con

    def free(self, con, err=None):
        """
        Args:
            con(SharedConnection):
            err

        Returns:
            bool
        """
        return super(PyMySQLPool, self).free((con, err))


class SingleConPool(object):
    def __init__(self, con):
        self._con = con

    def allocate(self, timeout=None):
        return self._con

    def free(self, con, err=None):
        pass
