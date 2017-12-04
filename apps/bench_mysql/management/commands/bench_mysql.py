#!coding: utf-8
import random
from typing import Callable, List, Dict
import time
import logging
from datetime import datetime
from queue import Queue, Empty
import uuid

import os

from pending.core.management.base import BaseCommand
from pending.db import new, Database
from pending.config import settings
from pending import log, sync
from influxdb import InfluxDBClient

from pending.framework import configure

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


class BatchActor(object):

    def __init__(self, perform: Callable, max_batch_size=10, max_wait_s=5):
        self._max_wait_s = max_wait_s
        self._max_batch_size = max_batch_size
        self._fn = perform
        self._queue = Queue()
        self._group = sync.ThreadGroup()
        self._group.go(self._sending, (), {})

    def _perform(self, data, max_retry=3):
        fail = True
        for i in range(max_retry):
            try:
                self._fn(data)
                fail = False
                break
            except Exception as e:
                logging.exception(u"err on performing data:%s,"
                                  u" err:%s, retry:%s",
                                  data, e.args, i + 1)
        return not fail

    def _sending(self):
        batch = []
        max_retry = 3
        need_send = False
        last_send_at = time.time()
        while not self._group.is_cancelled():
            try:
                data = self._queue.get(timeout=0.5)
                if data:
                    batch.append(data)
                    self._queue.task_done()
                if len(batch) >= self._max_batch_size:
                    need_send = True
                now = time.time()
                if now - last_send_at >= self._max_wait_s and batch:
                    need_send = True
                if need_send and batch:
                    if not self._perform(batch, max_retry):
                        logging.error("perform fail")
                    batch = []
                    last_send_at = now
                    need_send = False
            except Empty:
                pass
            except Exception as e:
                logging.exception(e.args)
        if batch:
            logging.info(
                "perform %s before to exit",
                self._perform(batch, max_retry) and "successfully" or "fail")
        logging.warning("bye")

    def send(self, target):
        self._queue.put(target)


def add_locations(locations, db, table):
    if not locations:
        return
    sql = "INSERT INTO `{table}`(car_id, tid, category, latitude," \
          " longitude, clatitude, clongitude, altitude, address, speed," \
          " degree, locate_error, snr, mcc, mnc, lac, cell_id," \
          " t_type, timestamp, rxlev, locate_type) VALUES ".format(table=table)
    values = []
    for location in locations:
        car_id = location.get('car_id', '')
        tid = location.get('tid', '')
        category = location.get('type', 1)
        latitude = location.get('latitude', 0)
        longitude = location.get('longitude', 0)
        clatitude = location.get('clatitude', 0)
        clongitude = location.get('clongitude', 0)
        altitude = location.get('altitude', 0)
        address = location.get('address', '')
        speed = location.get('speed', 0)
        degree = location.get('course', 0)
        locate_error = location.get('pacc', 50)
        snr = location.get('snr', 0)
        mcc = location.get('mcc', 0)
        mnc = location.get('mnc', 0)
        lac = location.get('lac', 0)
        cell_id = location.get('cell_id', 0)
        t_type = location.get('t_type', 0)
        timestamp = location.get('gps_timestamp', 0)
        locate_type = location.get('locate_type', 1)
        rxlev = location.get('rxlev', 0)
        value = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s'," \
                " '%s', '%s', '%s', '%s','%s', '%s', '%s','%s', '%s'," \
                " '%s', '%s', '%s')" % (car_id, tid, category, latitude,
                                        longitude, clatitude, clongitude,
                                        altitude, address, speed, degree,
                                        locate_error, snr, mcc, mnc, lac,
                                        cell_id, t_type, timestamp, rxlev,
                                        locate_type)
        values.append(value)
    sql += ",".join(values)
    last_insert_id = db.execute(sql)
    for location in locations:
        location["lid"] = last_insert_id
        last_insert_id += 1


def run(log_level='INFO', pid_file='/var/run/mysqld/mysqld.pid',
        inf_db='mysql_bench'):
    configure(log_level)
    log.set_tracking("RUN")

    group = sync.ThreadGroup()
    conf = settings.BENCH_MYSQL
    logging.warning("config:%s", conf)
    db_conf = conf["mysql"]
    db = new(**db_conf)
    sql = open(os.path.join(CURRENT_DIR, "./bench_mysql.sql")).read()
    result = db.execute(sql)
    logging.info("executing sql(result=%s):%s", result, sql)
    client = InfluxDBClient('localhost', 8086, 'root', 'root',
                            database=inf_db)
    client.create_database(inf_db)
    terms = [{"tid": uuid.uuid4().hex, "car_id": uuid.uuid4().hex}
             for _ in range(10 * 10000)]

    def perform_ibeats_sending(ibeat_tuples):
        points = [
            {
                "measurement": "t_mysql_bench",
                "tags": {
                    "id_type": "uuid",
                },
                "time": dt,
                "fields": ibeat,
            } for dt, ibeat in ibeat_tuples
        ]
        client.write_points(points, database='mysql_bench')
    sender = BatchActor(perform_ibeats_sending)

    def fn_send_ibeat(ibeat, dt):
        sender.send((dt, ibeat))

    # mysql_pid = int(open(pid_file).read())
    mysql_pid = 1
    try:
        group.go(
            sending_locations,
            (group.stopped, terms, db, fn_send_ibeat), {}
        )
        group.go(
            stat_mysql,
            (group.stopped, mysql_pid, db, fn_send_ibeat), {}
        )
        group.join()
    except (KeyboardInterrupt, SystemExit) as e:
        logging.warning("canncelling by `%s`", e)
        group.cancel()
        group.join(10)
    except Exception as e:
        logging.exception(e.args)
    finally:
        logging.warning("bye")


def sending_locations(cancelled, terms: List[Dict], db: Database, send_ibeat):
    log.set_tracking("SEND_LOCS")
    timestamp_cur = int(time.time())
    size_per_s = 10000
    sleep_s = 0.1
    while not cancelled.isSet():
        try:
            batch = []
            batch_size = int(size_per_s / (1/sleep_s))
            for i in [random.randint(0, len(terms) - 1)
                      for _ in range(batch_size)]:
                term = terms[i]
                tid = term["tid"]
                car_id = term["car_id"]
                location = {
                    "tid": tid,
                    "car_id": car_id,
                    "category": random.randint(0, 20),
                    "latitude": 23 * 3600000,
                    "longitude": 114 * 3600000,
                    "speed": 120,
                    "locate_type": random.randint(0, 3),
                    "locate_error": 1000,
                    "snr": 10,
                    "degree": 50,
                    "timestamp": timestamp_cur,
                }
                batch.append(location)
            timestamp_cur += 1
            begin = time.time()
            add_locations(batch, db, table='t_location')
            cost = int((time.time() - begin) * 1000)
            send_ibeat({
                "sending_cnt": batch_size,
                "sending_cost": cost
            }, datetime.utcnow())
            time.sleep(sleep_s)
        except Exception as e:
            logging.exception(u"err:%s", e.args)
    logging.warning("bye")


def stat_mysql(cancelled, pid, db: Database, send_ibeat, interval=2):
    """
    周期统计mysql: 进程io信息,Innodb引擎信息
    """
    log.set_tracking("STAT")

    def get_last_row_id(_db):
        last_row = db.get("select id from t_location ORDER BY id DESC LIMIT 1") \
            or {"id": 0}
        return last_row["id"]

    last_id = get_last_row_id(db)
    next_t = time.time() + interval
    while not cancelled.isSet():
        ts = int(time.time())
        wait_s = next_t - ts
        if wait_s > 0:
            time.sleep(wait_s)
            continue
        begin = time.time()
        now = datetime.utcfromtimestamp(ts)

        # 位置表行增长
        current_row_id = get_last_row_id(db)
        row_incr = current_row_id - last_id
        last_id = current_row_id

        ibeat = dict(row_incr=row_incr)
        ibeat["syscr"] = 0
        # with open("/proc/{pid}/io".format(pid=pid)) as fi:
        #     for line in fi.readlines():
        #         key, value = line.split(':')
        #         ival = int(value)
        #         ibeat[key] = ival

        ibeat.update({row["Variable_name"]: int(row["Value"])
                      for row in db.query("show status like '%%sync%%'")})
        send_ibeat(ibeat, now)
        cost = time.time() - begin
        if 2 * cost > interval:
            logging.warning(u"stat cost %s is too much, interval %s",
                            cost, interval)
        logging.info("send ibeat:%s, cost:%s", ibeat, cost)
        next_t = ts + interval
    logging.warning("bye")


class Command(BaseCommand):
    help = 'command to start a checker process'

    def add_arguments(self, parser):
        # show_sql
        parser.add_argument(
            '--show-sql',
            dest='show_sql',
            default=False,
            action="store_true",
            help='show sql ?'
        )
        # logging
        parser.add_argument(
            '--logging',
            '-l',
            dest='logging',
            default="INFO",
            help='[DEBUG, INFO,...] logging level'
        )

    def handle(self, *args, **options):
        show_sql = bool(options["show_sql"])
        log_level = str(options["logging"]).upper()

        run(log_level)
