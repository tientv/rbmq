import json
import os
from contextlib import contextmanager

from pika import BlockingConnection

import pika

EXCHANGE = "thp"


@contextmanager
def rbmq_conn(queue):
    url = os.environ.get("CLOUDAMQP_URL", "")
    params = pika.URLParameters(url)
    connection = BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type='direct',
        durable=True,
        auto_delete=False
    )
    channel.queue_declare(
        queue=queue,
        durable=True,
        auto_delete=False
    )
    channel.queue_bind(
        queue=queue,
        exchange=EXCHANGE,
        routing_key=queue
    )
    try:
        yield channel
    finally:
        channel.close()
        connection.close()


def push(queue, data):
    with rbmq_conn(queue) as channel:
        if not isinstance(data, str):
            data = json.dumps(data)
        channel.basic_publish(exchange=EXCHANGE,
                              routing_key=queue,
                              body=data.encode("utf-8"))


def start_consumer(queue, consumer_fn) -> Exception or None:
    with rbmq_conn(queue) as channel:
        channel.basic_consume(queue=queue,
                              on_message_callback=consumer_fn,
                              auto_ack=True)
        channel.start_consuming()
