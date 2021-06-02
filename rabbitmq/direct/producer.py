#!/usr/bin/env python3

"""
This module implements a simple RabbitMQ consumer using a direct exchange.
"""

import pika
import config


def start_producer():
    # Message to publish
    message = "Hello from producer!"

    # Create a new instance of the Connection object
    credentials = pika.PlainCredentials(config.USERNAME, config.PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   credentials=credentials))

    # Create a new channel
    channel = connection.channel()

    # Declare the queue (create if doesn't exists)
    channel.queue_declare(queue=config.DIRECT_EXCHANGE_QUEUE_NAME)

    # Publish the message
    channel.basic_publish(exchange="",
                          routing_key=config.DIRECT_EXCHANGE_QUEUE_NAME,
                          body=str.encode(message))

    # Log published message
    print("Message published successfully")
    print("Queue: " + config.DIRECT_EXCHANGE_QUEUE_NAME)
    print("Body: " + message)


if __name__ == "__main__":
    # Start the producer
    start_producer()
