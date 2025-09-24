#!/usr/bin/env python
# coding: utf-8

# Stock modules
import os
import sys
import logging
import argparse
import pika
import json

DESCRIPTION = """
Allows a data ingest process to send a new file notification to the GeoIPS
RabbitMQ server that accepts those kinds of notifications. The notification
will ultimately be used to add the file metadata to the GeoIPS Data Inventory
DB.
"""

log = logging.getLogger(__name__)


def produce_notification(
    filepath,
    start_time=None,
    end_time=None,
    length=None,
    checksum=None,
    checksum_type=None,
):
    """
    Send a "Fair Dispatch" message via RabbitMQ
    """

    # Establish connection and create a channel on that connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get("HOST"))
    )
    channel = connection.channel()

    # Ensure the durable file_notif_queue exists
    channel.queue_declare(queue="file_notif_queue", durable=True)

    # Put the message data in a dictionary for conversion to JSON
    msg_dict = {
        "filepath": filepath,
        "start_time": start_time,
        "end_time": end_time,
        "length": length,
        "checksum": checksum,
        "checksum_type": checksum_type,
    }

    msg_json = json.dumps(msg_dict)

    # Send the JSON formatted message
    channel.basic_publish(
        exchange="",
        routing_key="file_notif_queue",
        body=msg_json,
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
    )
    log.debug(f" [x] Sent {msg_json}")

    # Close the connection to make sure the message actually gets sent - buffers
    # are flushed
    connection.close()


def main():

    # Parse the arguments
    parser = argparse.ArgumentParser(f"{DESCRIPTION}python file_notification.py")

    # Add the positional argument(s?)
    parser.add_argument(
        "filepath", type=str, help="Send a notification for the file with this path."
    )

    # Add the flags
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output - set log level to DEBUG",
    )

    parser.add_argument(
        "-s",
        "--start_time",
        default=None,
        help="The first date and time for which the file has data",
    )

    parser.add_argument(
        "-e",
        "--end_time",
        default=None,
        help="The last date and time for which the file has data",
    )

    parser.add_argument(
        "-l", "--length", default=None, help="The length(size) of the file"
    )

    parser.add_argument("-c", "--checksum", default=None, help="The file's checksum")

    parser.add_argument(
        "-t",
        "--checksum_type",
        default=None,
        help="The type of the checksum - its algorithm",
    )

    pargs = parser.parse_args()

    # Setup logging.
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s%(name)s: %(message)s",
        level="DEBUG" if pargs.verbose else "INFO",
    )

    produce_notification(
        pargs.filepath,
        pargs.start_time,
        pargs.end_time,
        pargs.length,
        pargs.checksum,
        pargs.checksum_type,
    )


if __name__ == "__main__":
    log.info("Sending new file notification to GeoIPS")
    main()
