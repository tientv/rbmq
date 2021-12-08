# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import signal
import sys
import time
from threading import Thread
from contextlib import contextmanager

import numpy as np

from rbmq import queue

received_signal = False
processing_callback = False


def signal_handler(signal, frame):
    global received_signal
    print("signal received")
    received_signal = True
    if not processing_callback:
        sys.exit()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@contextmanager
def block_signals():
    global processing_callback
    processing_callback = True
    try:
        yield
    finally:
        processing_callback = False
        if received_signal:
            sys.exit()


def consumer_rdb_simple(ch, method, properties, body):
    with block_signals():
        print(" [x] consumer_rdb_simple " + body.decode("utf-8"))
        time.sleep(10)
        print("done", body.decode("utf-8"))
        queue.push("rdb_full", body.decode("utf-8"))


def consumer_rdb_full(ch, method, properties, body):
    with block_signals():
        print(" [x] consumer_rdb_full " + body.decode("utf-8"))
        queue.push("rdb_2mins", body.decode("utf-8"))
        queue.push("hdb", body.decode("utf-8"))


def consumer_rdb_2mins(ch, method, properties, body):
    with block_signals():
        print(" [x] consumer_rdb_2mins " + body.decode("utf-8"))
        time.sleep(2)
        queue.push("rdb_2mins", body.decode("utf-8"))


def consumer_hdb(ch, method, properties, body):
    with block_signals():
        print(" [x] consumer_hdb " + body.decode("utf-8"))


def start_queue_consumer(queue_name, consumer_fn, size):
    for _ in np.arange(size):
        t = Thread(target=queue.start_consumer, args=(queue_name, consumer_fn,))
        t.start()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        start_queue_consumer('rdb_simple', consumer_rdb_simple, 10)
        start_queue_consumer('rdb_full', consumer_rdb_full, 10)
        start_queue_consumer('rdb_2mins', consumer_rdb_2mins, 10)
        start_queue_consumer('hdb', consumer_hdb, 10)
        time.sleep(20)
        queue.push("rdb_simple", "0x0")
        queue.push("rdb_simple", "0x1")
        queue.push("rdb_simple", "0x2")
        queue.push("rdb_simple", "0x3")
        queue.push("rdb_simple", "0x4")
    except Exception as e:
        print(e)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
