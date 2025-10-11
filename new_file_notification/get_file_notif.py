#!/usr/bin/env python
import sys, os
import logging
import argparse
import pika
import json
import configparser
from functools import partial

# GeoIPS modules: the data inventory client
from data_inv_api import DIClient

DESCRIPTION = """
Receives a new file notification from the GeoIPS RabbitMQ "New File
Notification" server, and adds the file metadata to the GeoIPS Data Inventory
DB.
"""

log = logging.getLogger(__name__)


def consume_notification(config):
    # Establish connection and create a channel on that connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config["Settings"]["RMQ_HOST"])
    )
    channel = connection.channel()

    # Ensure the durable task_queue exists
    channel.queue_declare(queue="file_notif_queue", durable=True)
    log.info(" [*] Waiting for messages. To exit press CTRL+C")

    # Create the recieve callback function
    def callback(ch, method, properties, body, custom_object):
        file_info = json.loads(body.decode())
        log.info(f" [x] Received file_info: {file_info}")
        fname = os.path.basename(file_info['filepath'])
        rows = dic.find_files(filenames = fname)
        for row in rows:
            log.info('Got a DB row')
            log.info(f"Before: file_name: {row.get('file_name')}, location: {row.get('location')}, dir_path: {row.get('dir_path')}")

        tstart = '2013-06-29 11:18:33'
        tend = '2013-06-29 12:57:33'
        upsert_fpath = os.path.join("/data_store", file_info['filepath'][1:])
        result = dic.upsert_file(upsert_fpath, file_info['product'], file_info['version'], tstart, tend, size=0)
        log.info(f"upsert result: {result}")

        rows = dic.find_files(filenames = fname)
        for row in rows:
            log.info('Got a DB row')
            log.info(f"After: file_name: {row.get('file_name')}, location: {row.get('location')}, dir_path: {row.get('dir_path')}")

        log.info(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Create the data inventory client object and allow it to be sent to the
    # rabbitmq callback
    dic = DIClient(user='geoips', data_mount_dir='/data_store')
    bound_callback = partial(callback, custom_object=dic)

    # Set up "whichever's ready" dispatching
    # Register the callback function with rabbitmq
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="file_notif_queue",
      on_message_callback=bound_callback)

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

    # Reduce pika logging
    logging.getLogger("pika").setLevel(logging.WARNING)

    # Read the configuration file
    config = configparser.ConfigParser()
    try:
        config.read("config.ini")
    except FileNotFoundError:
        log.error("config.ini not found. Please ensure the file exists.")
        exit()

    consume_notification(config)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
