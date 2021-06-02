#!/usr/bin/env python3

"""
This module implements a simple RabbitMQ consumer using a topic exchange.

Routing key pattern -> country.sport.type

Examples:
    Tennis Events -> *.tennis.*
    Spain Events -> es.*.* or es.#
    All Events -> #
"""

import sys
import pika
import config

from pika.exchange_type import ExchangeType


def callback(ch, method, properties, body):
    # Log consumed message
    print()
    print(f"Message received: {body.decode('utf-8')}")
    print(f"Routing key: {method.routing_key}")


def start_consumer(routing_key):
    # Create a new instance of the Connection object
    credentials = pika.PlainCredentials(config.USERNAME, config.PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   credentials=credentials))

    # Create a new channel
    channel = connection.channel()

    # Create topic exchange
    channel.exchange_declare(exchange=config.TOPIC_EXCHANGE_NAME,
                             exchange_type=ExchangeType.topic.value)

    # Create the queue and associate to the exchange
    queue_name = channel.queue_declare(queue='').method.queue
    channel.queue_bind(queue=queue_name, exchange=config.TOPIC_EXCHANGE_NAME, routing_key=routing_key)

    # Subscribe to the queue
    channel.basic_consume(queue=queue_name,
                          on_message_callback=callback,
                          auto_ack=True)

    # Log waiting message
    print('Waiting for messages. To exit press CTRL+C.')

    # Start consuming messages
    channel.start_consuming()


if __name__ == "__main__":
    # Get input arguments
    routing_key_argv = sys.argv[1] \
        if len(sys.argv) > 1 else "#"

    # Start the consumer
    start_consumer(routing_key_argv)
