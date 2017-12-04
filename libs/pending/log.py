#!coding: utf-8
import os
import sys
import copy
import logging
from collections import OrderedDict
from tornado.log import LogFormatter
from threading import local
_local = local()


if hasattr(sys, 'frozen'):
    # support for py2exe
    _srcfile = "pending%slog%s" % (os.sep, __file__[-4:])
elif __file__[-4:].lower() in ['.pyc', '.pyo']:
    _srcfile = __file__[:-4] + '.py'
else:
    _srcfile = __file__
_srcfile = os.path.normcase(_srcfile)


# next bit filched from 1.5.2's inspect.py
def currentframe():
    """Return the frame object for the caller's stack frame."""
    try:
        raise Exception
    except:
        return sys.exc_info()[2].tb_frame.f_back


if hasattr(sys, '_getframe'): currentframe = lambda: sys._getframe(3)


def find_caller():
    """
    Find the stack frame of the caller so that we can note the source
    file name, line number and function name.
    """
    f = currentframe()
    # On some versions of IronPython, currentframe() returns None if
    # IronPython isn't run with -X:Frames.
    if f is not None:
        f = f.f_back
    rv = "(unknown file)", 0, "(unknown function)"
    while hasattr(f, "f_code"):
        co = f.f_code
        filename = os.path.normcase(co.co_filename)
        if filename == _srcfile:
            f = f.f_back
            continue
        rv = (co.co_filename, f.f_lineno, co.co_name)
        break
    return rv


class Logger(logging.Logger):

    def __init__(self, name, level=logging.NOTSET, fields=None, tags=None,
                 handlers=None):
        """

        Args:
            fields(OrderedDict)
            tags(list)
            handlers(list)
        """
        super(Logger, self).__init__(name, level)
        self.handlers = handlers or []
        self._fields = fields or OrderedDict()
        self._tags = tags or []

    def with_fields(self, key, value):
        """
        Args:
            key(str)
            value(object):
        Returns:
            Logger
        """
        fields = self._fields.copy()
        fields.update({key: value})
        tags = copy.copy(self._tags)
        return Logger(self.name, self.level, fields, tags,
                      handlers=self.handlers)

    def with_tags(self, *tags):
        """
        Returns:
            Logger
        """
        fields = self._fields.copy()
        _tags = copy.copy(self._tags)
        for tag in tags:
            if tag not in _tags:
                _tags.append(tag)
        return Logger(self.name, self.level, fields, _tags,
                      handlers=self.handlers)

    def _make_msg(self, msg):
        fields_s = u"".join([u"[%s:%s]" % (k, v)
                             for k, v in self._fields.items()])
        tags_s = u"".join([u"[%s]" % k for k in self._tags])
        msg = u"".join([fields_s, tags_s, msg])
        return msg

    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None, sinfo=None):

        msg = self._make_msg(msg)
        return super(Logger, self).makeRecord(
            name, level, fn, lno, msg, args, exc_info, func, extra)


def _resolve_tracking_msg(msg):
    tracking = getattr(_local, "tracking_info", "")
    return "%s%s" % (tracking, msg)


def _log_py2(self, level, msg, args, exc_info=None, extra=None):
    """
    Low-level logging routine which creates a LogRecord and then calls
    all the handlers of this logger to handle the record.
    """
    if _srcfile:
        #IronPython doesn't track Python frames, so findCaller raises an
        #exception on some versions of IronPython. We trap it here so that
        #IronPython can use logging.
        try:
            fn, lno, func = self.findCaller()
        except ValueError:
            fn, lno, func = "(unknown file)", 0, "(unknown function)"
    else:
        fn, lno, func = "(unknown file)", 0, "(unknown function)"
    if exc_info:
        if not isinstance(exc_info, tuple):
            exc_info = sys.exc_info()
    msg = _resolve_tracking_msg(msg)
    record = self.makeRecord(self.name, level, fn, lno, msg, args, exc_info, func, extra)
    self.handle(record)


def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False):
    """
    Low-level logging routine which creates a LogRecord and then calls
    all the handlers of this logger to handle the record.
    """
    sinfo = None
    if _srcfile:
        #IronPython doesn't track Python frames, so findCaller raises an
        #exception on some versions of IronPython. We trap it here so that
        #IronPython can use logging.
        try:
            fn, lno, func, sinfo = self.findCaller(stack_info)
        except ValueError: # pragma: no cover
            fn, lno, func = "(unknown file)", 0, "(unknown function)"
    else: # pragma: no cover
        fn, lno, func = "(unknown file)", 0, "(unknown function)"
    if exc_info:
        if isinstance(exc_info, BaseException):
            exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
        elif not isinstance(exc_info, tuple):
            exc_info = sys.exc_info()
    msg = _resolve_tracking_msg(msg)
    record = self.makeRecord(self.name, level, fn, lno, msg, args,
                             exc_info, func, extra, sinfo)
    self.handle(record)


def set_tracking(*tags):
    if not tags:
        tracking = ""
    else:
        tracking = "".join(["[%s]" % str(tag) for tag in tags])
    setattr(_local, "tracking_info", tracking)


def add_tracking(*tags):
    tracking = "".join(["[%s]" % str(tag) for tag in tags])
    exists = getattr(_local, "tracking_info", "")
    if len(exists) + len(tracking) > 1000:
        logging.warning("add_tracking fail, too long")
        return
    setattr(_local, "tracking_info", "%s%s" % (exists, tracking))


def patch_logging_for_tracking():
    import logging
    import six
    if six.PY2:
        log_fn = _log_py2
    else:
        log_fn = _log
    setattr(logging.RootLogger, "_log", log_fn)
    logging.root = logging.RootLogger(logging.WARNING)
    logging.Logger.manager = logging.Manager(logging.root)
    setattr(logging.Logger, "_log", log_fn)


def enable_pretty_logging(level=logging.INFO, logger=None, force=False):
    """Turns on formatted logging output as configured.
    """
    if logger is None:
        logger = logging.getLogger()
    logger.setLevel(level)
    if not logger.handlers or force:
        logger.handlers = []
        channel = logging.StreamHandler()
        channel.setFormatter(LogFormatter())
        logger.addHandler(channel)
    else:
        logging.warning("enable pretty logging fail,"
                        " logger.handlers isn't empty")
