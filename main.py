# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import signal
import sys
import time
from threading import Thread
from contextlib import contextmanager

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
        time.sleep(2)
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    try:
        t_rdb_simple = Thread(target=queue.start_consumer, args=("rdb_simple", consumer_rdb_simple,))
        t_rdb_simple.start()
        t_rdb_full = Thread(target=queue.start_consumer, args=("rdb_full", consumer_rdb_full,))
        t_rdb_full.start()
        t_rdb_2mins = Thread(target=queue.start_consumer, args=("rdb_2mins", consumer_rdb_2mins,))
        t_rdb_2mins.start()
        t_hdb = Thread(target=queue.start_consumer, args=("hdb", consumer_hdb,))
        t_hdb.start()
    except Exception as e:
        print(e)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
