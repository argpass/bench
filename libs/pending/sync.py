#!coding: utf-8
import sys
import time
import logging
from threading import Thread, Lock, Condition, Event, RLock

import math

import os

_logger = logging.getLogger(__name__)


CHECKING_DELAY_S = 10
SLEEP_ON_ERR = 1


class Timeout(Exception):
    pass


_Unset = object()


class ConsumingWish(object):
    def __init__(self):
        self._data = _Unset
        self._con = Event()

    def wait(self, timeout=None):
        return self._con.wait(timeout)

    def get(self):
        if self._data is _Unset:
            raise ValueError(u"consuming wish is un resolved")
        return self._data

    def feed(self, data):
        self._data = data
        return self._con.set()


class ProducingWish(object):
    def __init__(self, data):
        self._data = data
        self._con = Event()

    def wait(self, timeout=None):
        return self._con.wait(timeout)

    def get(self):
        self._con.set()
        return self._data


class Channel(object):
    def __init__(self):
        self._mutex = Lock()
        self._waiting_producers = []
        self._waiting_consumers = []

    def _trigger_resolving(self):
        with self._mutex:
            if self._waiting_consumers and self._waiting_producers:
                p = self._waiting_producers.pop()
                c = self._waiting_consumers.pop()
                c.feed(p.get())

    def send(self, data, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError(u"'timeout' must be a non-negative number")
        wish = ProducingWish(data)
        with self._mutex:
            self._waiting_producers.append(wish)
        self._trigger_resolving()
        if timeout is None:
            wish.wait()
        else:
            wish.wait(timeout)

    def receive(self, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError(u"'timeout' must be a non-negative number")
        wish = ConsumingWish()
        with self._mutex:
            self._waiting_consumers.append(wish)
        self._trigger_resolving()
        if timeout is None:
            wish.wait()
        else:
            wish.wait(timeout)
        return wish.get()

    def __len__(self):
        return 0


class ResourceFactory(object):

    def is_valid(self, resource):
        """
        Returns:
            bool
        """
        raise NotImplementedError(u"Implement the method by subclass")

    def new(self):
        """ create a new one
        Returns:

        """
        raise NotImplementedError(u"Implement the method by subclass")

    def free(self, resource):
        """
        Returns:
            bool
        """
        raise NotImplementedError(u"Implement the method by subclass")


class ResourcePoolBase(object):

    class Timeout(Exception):
        pass

    def __init__(self, maximum, idle_maximum, factory):
        """
        Args:
            factory(ResourceFactory)
            maximum(int)
        """
        if idle_maximum <= 0:
            raise ValueError(u"invalid idle_maximum value")
        if maximum <= 0:
            raise ValueError(u"invalid maximum value")
        self._maximum = maximum
        self._idle_maximum = idle_maximum
        self._hold_cnt = 0
        self._factory = factory
        self._alloc_chan = Channel()
        self._free_chan = Channel()
        self._can_check = Condition()
        self._resources = []
        self._start()

    def _start(self):
        alloc_th = Thread(target=self._allocating, name="pool_alloc")
        free_th = Thread(target=self._freeing, name="pool_free")
        check_th = Thread(target=self._checking, name="pool_check")
        check_th.setDaemon(True)
        check_th.start()
        free_th.setDaemon(True)
        free_th.start()
        alloc_th.setDaemon(True)
        alloc_th.start()

    def _checking(self):
        while True:
            # check per 3 seconds
            time.sleep(CHECKING_DELAY_S)
            try:
                # this isn't correct,
                # there may be a connection in queue `_alloc_queue`
                if len(self._resources) > self._idle_maximum:
                    self._resources, to_free = \
                        self._resources[0: self._idle_maximum], \
                        self._resources[self._idle_maximum:]
                    for con in to_free:
                        self.__free(con)
            except Exception as e:
                _logger.error(e)

    def __free(self, obj):
        self._factory.free(obj)
        self._hold_cnt -= 1

    def _allocating(self):
        while True:
            try:
                try:
                    obj = self._resources.pop()
                except IndexError:
                    if self._hold_cnt < self._maximum:
                        obj = self._factory.new()
                        self._hold_cnt += 1
                    else:
                        # maximum exceed, wait freeing thread
                        self._can_check.acquire()
                        self._can_check.wait()
                        self._can_check.release()
                        continue
                self._alloc_chan.send(obj)
            except Exception as e:
                _logger.exception(e)
                # exceptions occurs, wait a moment to continue
                time.sleep(SLEEP_ON_ERR)

    def _freeing(self):
        while True:
            try:
                obj = self._free_chan.receive()
                self._resources.append(obj)

                # notify allocating thread
                self._can_check.acquire()
                self._can_check.notify_all()
                self._can_check.release()

            except Exception as e:
                _logger.exception(e)
                # exceptions occurs, wait a moment to continue
                time.sleep(SLEEP_ON_ERR)

    def allocate(self, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError(u"timeout must be negative")
        end_time = 3600 * 24 * 365 * 100
        if timeout:
            end_time = time.time() + timeout
        while True:
            if timeout is None:
                resource = self._alloc_chan.receive()
            else:
                remaining = end_time - time.time()
                if remaining < 0.0:
                    raise Timeout()
                resource = self._alloc_chan.receive(remaining)
            if self._factory.is_valid(resource):
                return resource
            else:
                self.__free(resource)

    def free(self, resource):
        self._free_chan.send(resource)


class ThreadSharedPool(ResourcePoolBase):
    pass


def join_threads_gracefully(threads, timeout=None):
    """
    Args:
        threads(list[Thread]):
        timeout:
    Returns:
        None|int:
    """
    timeout = 3600 * 24 * 365 * 100 if timeout is None else timeout
    if timeout < 0:
        raise ValueError(u"invalid timeout value `%s`" % repr(timeout))
    end_time = time.time() + timeout
    count = 0
    for th in threads:
        remaining = end_time - time.time()
        if remaining < 0.0:
            break
        th.join(remaining)
        if not th.isAlive():
            count += 1
    return len(threads) - count


def sleep(t, cancelled):
    next_t = t + time.time()
    while not cancelled.isSet():
        now = time.time()
        if now >= next_t:
            # timeout
            return True
        time.sleep(0.05)
        # continue
    # cancelled
    return False


class ThreadGroup(object):
    def __init__(self):
        self._is_running = Event()
        self._threads = []
        self._stopped = Event()

    def go(self, fn, args=None, kwargs=None):
        args = args or ()
        kwargs = kwargs or dict()
        th = Thread(target=fn, args=args, kwargs=kwargs)
        th.setDaemon(True)
        th.start()
        self._threads.append(th)

    def cancel(self):
        self._stopped.set()

    def is_cancelled(self):
        return self._stopped.isSet()

    @property
    def stopped(self):
        return self._stopped

    def join(self, timeout=None):
        return join_threads_gracefully(self._threads, timeout)


class ThreadSafeIter:
    def __init__(self, it):
        self.it = it
        self._cnt = 0
        self.lock = RLock()

    def __iter__(self):
        return self

    @property
    def count(self):
        return self._cnt

    def next(self):
        with self.lock:
            self._cnt += 1
            return self.it.next()
