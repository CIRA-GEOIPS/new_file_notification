#!/usr/bin/env python
import sys, os
import logging
import argparse
import pika
import json

DESCRIPTION = """
Receives a new file notification from the GeoIPS RabbitMQ "New File
Notification" server, and adds the file metadata to the GeoIPS Data Inventory
DB.
"""

log = logging.getLogger(__name__)


def consume_notification():
    # Establish connection and create a channel on that connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("HOST"))
    )
    channel = connection.channel()

    # Ensure the durable task_queue exists
    channel.queue_declare(queue="file_notif_queue", durable=True)
    log.info(" [*] Waiting for messages. To exit press CTRL+C")

    # Create the recieve callback function
    def callback(ch, method, properties, body):
        file_info = json.loads(body.decode())
        log.info(f" [x] Received file_info: {file_info}")
        log.debug(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Set up "whichever's ready" dispatching
    # Register the callback function with rabbitmq
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="file_notif_queue", on_message_callback=callback)

    # Start the message checking loop
    channel.start_consuming()


def main():
    # Parse the arguments
    parser = argparse.ArgumentParser(f"{DESCRIPTION}python get_file_notif.py")

    # Add the flags
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output - set log level to DEBUG",
    )

    pargs = parser.parse_args()

    # Setup logging.
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s%(name)s: %(message)s",
        level="DEBUG" if pargs.verbose else "INFO",
    )

    consume_notification()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
