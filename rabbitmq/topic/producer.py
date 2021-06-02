#!/usr/bin/env python3

"""
This module implements a simple RabbitMQ consumer using a topic exchange.
"""

import pika
import random

from time import sleep
from pika.exchange_type import ExchangeType

import config


def start_producer():
    # Create a new instance of the Connection object
    credentials = pika.PlainCredentials(config.USERNAME, config.PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   credentials=credentials))

    # Create a new channel
    channel = connection.channel()

    # Create topic exchange
    channel.exchange_declare(exchange=config.TOPIC_EXCHANGE_NAME,
                             exchange_type=ExchangeType.topic.value)

    # Create topics lists
    countries = ["es", "fr", "usa"]
    sports = ["football", "tennis", "skiing"]
    event_types = ["live", "news"]

    # Send a message to the exchange every second
    counter = 1

    while True:
        # Shuffle the topics
        random.shuffle(countries)
        random.shuffle(sports)
        random.shuffle(event_types)

        # Get the first topic for each list
        country = countries[0]
        sport = sports[0]
        event_type = event_types[0]

        # Generate the routing key (country.sport.type)
        routingKey = country + "." + sport + "." + event_type

        # Create the message
        message = f"Event {counter}"

        # Send the message to the exchange
        channel.basic_publish(exchange=config.TOPIC_EXCHANGE_NAME,
                              routing_key=routingKey,
                              body=str.encode(message))

        # Log published message
        print(f"Message published ({routingKey}): {message}")

        # Sleep for 2 seconds
        sleep(2)

        # Increase the counter
        counter += 1


if __name__ == "__main__":
    # Start the producer
    start_producer()
