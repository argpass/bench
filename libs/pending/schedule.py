#!coding: utf-8
from __future__ import absolute_import

from Queue import Queue, Empty
import json
from collections import defaultdict
from threading import Event, RLock, Thread
from pending import log, sync
import logging
import uuid
import time
from utils.myredis import MyRedis


logger = logging.getLogger(__name__)


class JobConfig(object):
    trigger = None
    executor = None
    job_id = None

    # optional
    verbose_name = None

    def run_once(self, *args, **kwargs):
        """任务执行一趟"""
        raise NotImplementedError(u"Implement the method by subclass")


job_attr_required = ("trigger", "executor", "job_id")


def _build_job(job_class, args, kwargs):
    """
    Returns:
        Job
    """
    args = args or ()
    kwargs = kwargs or {}
    job_config = job_class(*args, **kwargs)
    job = Job(fn=job_config.run_once,
              job_id=job_config.job_id,
              trigger=job_config.trigger,
              executor=job_config.executor,
              verbose_name=job_config.verbose_name or job_config.job_id, )
    return job


class Job(object):
    __slots__ = ("fn", "job_id", "verbose_name", "trigger",
                 "next_time", "executor")

    def __init__(self, fn, job_id, trigger, executor, verbose_name=None):
        """
        Args:
            fn:
            job_id(str):
            trigger(Trigger):
            executor(Executor):
            verbose_name:
        """
        self.fn = fn
        self.job_id = job_id
        self.trigger = trigger
        self.verbose_name = verbose_name or job_id
        next_times = self.trigger.get_next_times()
        if not next_times:
            raise ValueError(u"invalid job with no chance to be executed")
        self.next_time = next_times[0]
        self.executor = executor

    def __setstate__(self, state):
        for key in self.__slots__:
            setattr(self, key, state[key])

    def __getstate__(self):
        return {k: getattr(self, k) for k in self.__slots__}


class Trigger(object):
    def get_next_times(self, previous=None, end=None):
        """
        Args:
            previous: previous time
            end: end time
        """
        raise NotImplementedError(u"Implement the method by subclass")


class IntervalTrigger(Trigger):
    def __init__(self, interval, begin_at=None):
        """
        Args:
            interval(int|float):
            begin_at(int|float):
        """
        self._interval = interval
        self._begin_at = begin_at if begin_at is not None \
            else time.time() + interval

    def get_next_times(self, previous=None, end=None):
        begin_at = previous + self._interval \
            if previous is not None else self._begin_at
        if begin_at is None:
            raise ValueError(u"unresolved `begin_at`")
        if end is None:
            return [begin_at]
        next_times = []
        next_t = begin_at
        while next_t <= end:
            next_times.append(next_t)
            next_t = next_t + self._interval
        return next_times


class JobRepository(object):
    def add_job(self, job):
        """添加job"""
        raise NotImplementedError(u"Implement the method by subclass")

    def pop_jobs(self, end_time):
        """弹出在end_time之前有执行机会的job列表"""
        raise NotImplementedError(u"Implement the method by subclass")

    def next_check_at(self):
        """下一个检查时间点"""
        raise NotImplementedError(u"Implement the method by subclass")


class StandaloneRepository(JobRepository):
    """使用全局锁实现全局单例仓库"""

    def __init__(self, lock):
        """
        Args:
            lock(GlobalLock):
        """
        self._next_check_t = None
        self._global_lock = lock
        # job_id => job config
        self._job_map = dict()
        # tuple(next_time, job_id)
        self._next_time_sorted = []

    def add_job(self, job):
        """
        Args:
            job(Job):

        Returns:
            StandaloneRepository
        """
        if job.job_id in self._job_map:
            raise RuntimeError(u"job already exists")
        if job.next_time is None:
            raise ValueError(u"invalid job with None of next_time")
        self._job_map[job.job_id] = job
        if self._next_check_t is None:
            self._next_check_t = job.next_time
        else:
            self._next_check_t = min(job.next_time, self._next_check_t)
        return self

    def next_check_at(self):
        return self._next_check_t

    def _remove(self, job):
        self._job_map.pop(job.job_id)

    def pop_jobs(self, end_time):
        _jobs = []
        next_times = []
        for job in self._job_map.values():
            if job.next_time <= end_time:
                run_times = [job.next_time]
                # check if exists extra run time between job.next_time and end_time
                run_times.extend(job.trigger.get_next_times(job.next_time, end_time))
                # append to returns
                _jobs.append((job, run_times))
                # calculate next run time
                job_next_times = job.trigger.get_next_times(run_times[-1], end=None)
                if not job_next_times:
                    # job would be fired
                    self._remove(job)
                    continue
                # update job.next_time
                job.next_time = job_next_times[0]
                next_times.append(job.next_time)
            else:
                next_times.append(job.next_time)

        # make min next_time as the `next_check_t`
        self._next_check_t = min(next_times)

        # check lock before to return jobs
        # warning: 一旦此刻未获得锁则次刻之前的执行时间都会被跳过
        return self._global_lock.wait(0) and _jobs or []


class GlobalLock(object):
    def __init__(self):
        self._got = Event()

    def wait(self, timeout=None):
        return self._got.wait(timeout)


class RedisGlobalLock(GlobalLock):
    def __init__(self, lock_key, redis, node_id=None, lock_interval=10):
        super(RedisGlobalLock, self).__init__()
        self._group = sync.ThreadGroup()
        self._lock_key = lock_key
        self._redis = redis
        self._is_running = False
        self._lock_interval = lock_interval
        self._node_id = node_id or uuid.uuid4().hex[:12]

    def _racing(self):
        logger.info(u"[GL] start racing")
        acquired = False
        while not self._group.is_cancelled():
            try:
                # ensure lock racing
                pp = self._redis.pipeline()
                pp.set(
                    self._lock_key,
                    json.dumps(
                        {"node_id": self._node_id, "t": int(time.time())}),
                    nx=True,
                    ex=self._lock_interval * 2,
                )
                pp.get(self._lock_key)
                success, lock_data = pp.execute()
                try:
                    node_id = json.loads(lock_data)["node_id"]
                except Exception as e:
                    logger.exception(u"[GL] invalid lock data format:`%s`, "
                                     u"err:%s",
                                     lock_data, e.args)
                    raise e
                else:
                    if node_id == self._node_id:
                        success = True
                if success:
                    # i'm the owner, send a heartbeat
                    pp = self._redis.pipeline()
                    pp.expire(self._lock_key, self._lock_interval)
                    pp.execute()
                    if not acquired:
                        # got lock
                        acquired = True
                        self._got.set()
                        logger.info(u"[GL] lock acquired")
                else:
                    if acquired:
                        # lost lock
                        acquired = False
                        self._got.clear()
                        logger.info(u"[GL] lock lost")
                # sleep a moment before next racing
                if not sync.sleep(self._lock_interval / 2, self._group.stopped):
                    # stopped
                    break
            except Exception as e:
                logger.exception(e.args)
                time.sleep(1)
        logger.warning(u"[GL] racing bye")

    def start(self):
        if self._is_running:
            raise RuntimeError(u"never to start a running instance")
        self._group.go(self._racing)

    def join(self, timeout=None):
        return self._group.join(timeout)

    def cancel(self):
        self._group.cancel()
        self._got.clear()


class Executor(object):
    def submit(self, job, run_times):
        raise NotImplementedError(u"Implement the method by subclass")

    def start(self):
        raise NotImplementedError(u"Implement the method by subclass")

    def cancel(self):
        raise NotImplementedError(u"Implement the method by subclass")

    def join(self, timeout=None):
        raise NotImplementedError(u"Implement the method by subclass")


def run_job(job, run_times):
    try:
        job.fn()
    except Exception as e:
        logger.exception(e.args)


class ThreadPoolExecutor(Executor):
    """单实例线程池,每个任务只允许一个在执行"""

    def __init__(self, worker_num=10):
        self._threads = []
        self._worker_num = worker_num
        self._is_running = False
        self._stopped = Event()
        self._task_queue = Queue()
        # job_id=>instance_count
        self._instances_counter = defaultdict(int)

    def start(self):
        if self._is_running:
            raise RuntimeError(u"already running")
        self._is_running = True
        self._threads.extend([Thread(target=self._running_task)
                              for _ in range(self._worker_num)])
        for th in self._threads:
            th.setDaemon(True)
            th.start()

    def _running_task(self):
        logger.debug(u"[TEP] running")
        while not self._stopped.isSet() or not self._task_queue.empty():
            try:
                job, run_times = self._task_queue.get(timeout=1)
                self._task_queue.task_done()
                try:
                    run_job(job, run_times)
                finally:
                    self._instances_counter[job.job_id] -= 1
            except Empty:
                continue
            except Exception as e:
                logger.exception(e.args)
        logger.debug(u"[TEP] bye")

    def cancel(self):
        self._stopped.set()

    def join(self, timeout=None):
        self._task_queue.join()
        return sync.join_threads_gracefully(
            threads=self._threads, timeout=timeout)

    def submit(self, job, run_times):
        if self._instances_counter[job.job_id] == 0:
            self._task_queue.put((job, run_times))
            self._instances_counter[job.job_id] += 1
        else:
            logger.debug(u"[TEP] an instance is running, "
                         u"ignore this submit")


class JobRunner(object):
    def __init__(self):
        self._group = sync.ThreadGroup()
        self._job_repositories = []

    def add_repository(self, repo):
        self._job_repositories.append(repo)
        return self

    def _loop_repository(self, repo):
        """
        Args:
            repo(JobRepository):
        """
        logger.debug(u"[LR]loop repository %s", repo)
        next_check_at = repo.next_check_at()
        while not self._group.is_cancelled():
            try:
                now = time.time()
                if next_check_at is None:
                    # there is no need to check the repo, repo is empty
                    break
                wait_s = next_check_at - now
                if wait_s > 0:
                    logger.debug(u"[LR]wait %s seconds", wait_s)
                    if self._group.stopped.wait(wait_s):
                        # group is cancelled
                        break
                    # timeout, next check
                    continue

                # reach next_check_time, try to pop jobs
                # time isn't the only condition, pop_jobs may return emtpy list
                jobs_and_run_times = repo.pop_jobs(now)
                # now try to get next jobs
                for job, run_times in jobs_and_run_times:
                    executor = job.executor
                    logger.debug(u"[LR] to submit `%s`, run_times `%s`",
                                 job.job_id, run_times)
                    executor.submit(job, run_times)
                next_check_at = repo.next_check_at()
                logger.debug(u"[LR] next_check_at:%s", next_check_at)
            except Exception as e:
                logger.exception(e.args)
                time.sleep(0.5)
        logger.warning(u"[LR] loop bye")

    def start(self):
        for repo in self._job_repositories:
            self._group.go(self._loop_repository, args=(repo,))

    def cancel(self):
        self._group.cancel()

    def join(self, timeout=None):
        return self._group.join(timeout)


class StandaloneJobRunner(object):
    """redis实现的单实例job runner"""

    _master_lock_heartbeat = 10

    def __init__(self, name, redis, node_id=None):
        """
        Args:
            redis(MyRedis):
            name(str): runner name
        """
        self._name = name
        self._redis = redis
        self._node_id = node_id or uuid.uuid4().hex[:12]

        self._stopped = Event()
        self._mu = RLock()
        self._is_running = False
        self._sig_queue = None
        self._jobs_map = dict()
        self._threads = []

    def register(self, name, job, verbose_name=None):
        self._mu.acquire()
        try:
            if name in self._jobs_map:
                raise ValueError(u"job %s registered" % (name,))
            if self._is_running:
                raise RuntimeError(u"please to register jobs before running")
            self._jobs_map[name] = {
                "job": job,
                "verbose_name": verbose_name or name,
            }
        finally:
            self._mu.release()

    def on_heartbeat_lost(self):
        self.cancel()

    def keeping_heartbeat(self):
        log.set_tracking("HB")
        logger.info("start")
        next_t = time.time()
        while not self._stopped.isSet():
            try:
                now = time.time()
                if now >= next_t:
                    # 每次心跳都要检查是不是自己的锁
                    lock_lost = False
                    meta = self._redis.get_value(
                        self.global_lock_key)
                    if not meta:
                        lock_lost = True
                    elif meta.get("node_id") != self._node_id:
                        lock_lost = True
                    if lock_lost:
                        logger.error("i'm not the locker")
                        self.on_heartbeat_lost()
                        continue

                    # keep heartbeat
                    self._redis.expire(self.global_lock_key,
                                       self._master_lock_heartbeat)
                    next_t = now + self._master_lock_heartbeat / 2.0
                else:
                    time.sleep(0.05)
            except Exception as e:
                logger.exception(e.args)
        logger.debug("bye")

    def run_job(self, name, job_map):
        log.set_tracking("TASK", name.upper())
        job = job_map["job"]
        next_t = time.time()
        while not self._stopped.isSet():
            now = time.time()
            try:
                if now >= next_t:
                    next_t = now + job.interval
                    job.run_once()
                time.sleep(0.1)
            except Exception as e:
                logger.exception(e.args)
        logger.debug("bye")

    def bootstrap(self):
        self._mu.acquire()
        try:
            if self._is_running:
                raise RuntimeError(u"never to bootstrap a running instance")
            self._is_running = True
            # ensure lock racing
            pp = self._redis.pipeline()
            pp.set(
                self.global_lock_key,
                json.dumps({"node_id": self._node_id, "t": int(time.time())}),
                nx=True,
                ex=self._master_lock_heartbeat * 2,
            )
            pp.get(self.global_lock_key)
            success, lock_data = pp.execute()
            try:
                node_id = json.loads(lock_data)["node_id"]
            except Exception as e:
                logger.exception(u"invalid lock data format:`%s`, err:%s",
                                  lock_data, e.args)
                raise e
            else:
                if node_id == self._node_id:
                    success = True
            if not success:
                meta = self._redis.get_value(self.global_lock_key)
                raise RuntimeError(u"an instance has already been running, "
                                   u"meta:%s" % repr(meta))

            logger.debug("lock got")
            # heartbeat thread
            heartbeat_thread = Thread(target=self.keeping_heartbeat)
            self._threads.append(heartbeat_thread)
            logger.info("bootstrapping jobs:%s", repr(self._jobs_map.keys()))

            # create a thread for every job
            for name, job_map in self._jobs_map.items():
                job_thread = Thread(target=self.run_job, args=(name, job_map))
                self._threads.append(job_thread)

            # start all thread
            for th in self._threads:
                th.setDaemon(True)
                th.start()
        finally:
            self._mu.release()

    def join(self, timeout=None):
        if timeout is None:
            for th in self._threads:
                th.join()
            return 0
        else:
            if timeout < 0:
                raise ValueError(u"invalid timeout value `%s`" % repr(timeout))
            end_time = time.time() + timeout
            count = 0
            for th in self._threads:
                remaining = end_time - time.time()
                if remaining < 0.0:
                    break
                th.join(remaining)
                if not th.isAlive():
                    count += 1
            return len(self._threads) - count

    def cancel(self):
        self._stopped.set()

    @property
    def global_lock_key(self):
        return "%s:_lock" % (self._name,)


def register_job(repo, args=None, kwargs=None):
    """Job Decorator
    Args:
        repo(JobRepository)
        args:
        kwargs:
    """
    args = args or ()
    kwargs = kwargs or {}

    def decorator(job_klass):
        # register job
        job = _build_job(job_klass, args, kwargs)
        repo.add_job(job)
        return job_klass

    return decorator
