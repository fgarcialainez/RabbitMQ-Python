#!/usr/bin/env python3

"""
This module implements a simple RabbitMQ consumer using a direct exchange.
"""

import pika
import config


def callback(ch, method, properties, body):
    # Log consumed message
    print("Message consumed successfully")
    print("Queue: " + config.DIRECT_EXCHANGE_QUEUE_NAME)
    print("Body: " + body.decode("utf-8"))


def start_consumer():
    # Create a new instance of the Connection object
    credentials = pika.PlainCredentials(config.USERNAME, config.PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   credentials=credentials))

    # Create a new channel
    channel = connection.channel()

    # Declare the queue (create if doesn't exists)
    channel.queue_declare(queue=config.DIRECT_EXCHANGE_QUEUE_NAME)

    # Subscribe to the queue
    channel.basic_consume(queue=config.DIRECT_EXCHANGE_QUEUE_NAME,
                          on_message_callback=callback,
                          auto_ack=True)

    # Log waiting message
    print('Waiting for messages. To exit press CTRL+C.')

    # Start consuming messages
    channel.start_consuming()


if __name__ == "__main__":
    # Start the consumer
    start_consumer()
