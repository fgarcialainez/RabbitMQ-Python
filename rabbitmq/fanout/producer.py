#!/usr/bin/env python3

"""
This module implements a simple RabbitMQ consumer using a fanout exchange.
"""

import pika
import config

from time import sleep
from pika.exchange_type import ExchangeType


def start_producer():
    # Create a new instance of the Connection object
    credentials = pika.PlainCredentials(config.USERNAME, config.PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   credentials=credentials))

    # Create a new channel
    channel = connection.channel()

    # Create fanout exchange
    channel.exchange_declare(exchange=config.FANOUT_EXCHANGE_NAME,
                             exchange_type=ExchangeType.fanout.value)

    # Send a message to the exchange every second
    counter = 1

    while True:
        # Create the message
        message = f"Event {counter}"

        # Send the message to the exchange
        channel.basic_publish(exchange=config.FANOUT_EXCHANGE_NAME,
                              routing_key="",
                              body=str.encode(message))

        # Log published message
        print(f"Message published: {message}")

        # Sleep for 2 seconds
        sleep(2)

        # Increase the counter
        counter += 1


if __name__ == "__main__":
    # Start the producer
    start_producer()
