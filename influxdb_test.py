#!coding: utf-8
import logging
import random
import time
from datetime import datetime

import uuid
from influxdb import InfluxDBClient

db = 'example'
client = InfluxDBClient('localhost', 8086, 'root', 'root', database=db)
client.create_database(db)


def perform_ibeats_sending():
    probability = {
        'U1': 5,
        'U2': 40,
        'U7': 60,
        'U9': 5,
        'U4': 5,
        'U3': 5,
        'U6': 5,
        'U10': 10
    }
    probability_sample_lst = []
    for k, v in probability.items():
        probability_sample_lst.extend([k for _ in range(v)])

    num = 0
    while True:
        t = int(time.time())
        # val = random.randint(0, 100)
        points = []
        for _ in range(500):
            msg_no = random.randint(0, len(probability_sample_lst) - 1)
            points.append({
                "measurement": "packet_cnt",
                "tags": {
                    "iccid": "iccid%s" % random.randint(1, 10 * 10000),
                    "message_no": "U%s" % probability_sample_lst[msg_no]
                },
                "time": datetime.utcfromtimestamp(t),
                "fields": {
                    "packet": '{packet%s content -------}' % msg_no,
                },
            })
        begin = time.time()
        if not client.write_points(points, database=db):
            logging.warning("write fail")
        else:
            num += 500
            logging.warning("num:%s, cost:%.3f", num, (time.time() - begin) * 1000)
        # logging.warning(client.query("select * from demo;", database=db))
        # time.sleep(0.5)


perform_ibeats_sending()
