#!/usr/bin/env python3

"""
This module implements a simple RabbitMQ consumer using a fanout exchange.
"""

import pika
import config

from pika.exchange_type import ExchangeType


def callback(ch, method, properties, body):
    # Log consumed message
    print(f"Message received: {body.decode('utf-8')}")


def start_consumer():
    # Create a new instance of the Connection object
    credentials = pika.PlainCredentials(config.USERNAME, config.PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   credentials=credentials))

    # Create a new channel
    channel = connection.channel()

    # Create fanout exchange
    channel.exchange_declare(exchange=config.FANOUT_EXCHANGE_NAME,
                             exchange_type=ExchangeType.fanout.value)

    # Create the queue and associate to the exchange
    queue_name = channel.queue_declare(queue='').method.queue
    channel.queue_bind(queue=queue_name, exchange=config.FANOUT_EXCHANGE_NAME)

    # Subscribe to the queue
    channel.basic_consume(queue=queue_name,
                          on_message_callback=callback,
                          auto_ack=True)

    # Log waiting message
    print('Waiting for messages. To exit press CTRL+C.')

    # Start consuming messages
    channel.start_consuming()


if __name__ == "__main__":
    # Start the consumer
    start_consumer()
