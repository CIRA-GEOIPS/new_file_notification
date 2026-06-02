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
from data_inv_api.errors import DIClientError, DIClientPgError

DESCRIPTION = """
Receives a new file notification from the GeoIPS RabbitMQ "New File
Notification" server, and adds the file metadata to the GeoIPS Data Inventory
DB.
"""

log = logging.getLogger(__name__)

def notif_callback(ch, method, properties, body, custom_object):
    """The recieve message callback function"""
    file_info = json.loads(body.decode())
    dic = custom_object
    log.info(f" [x] Received file_info: {file_info}")
    try:
        do_upsert = True
        fname = os.path.basename(file_info['filepath'])
        rows = dic.find_files(filenames = fname)
        for row in rows:
            log.info('Got a DB row')
            log.info(
                f"Before: file_name: {row.get('file_name')}, location:"
                f" {row.get('location')}, dir_path: {row.get('dir_path')},"
                f" size: {row.get('size')}"
            )
            db_fpath = os.path.join(row.get("dir_path"), row.get("file_name"))
            local_fpath = data_inv_api.pg_di_client.get_local_fpath(db_fpath, row.get("location"))
            curr_size = os.path.getsize(local_fpath)

            if (
                db_fpath == file_info['filepath'] and row.get('location') ==
                file_info['data_store'] and row.get('size') == curr_size
            ):
                do_upsert = False
                log.info(
                    f"row.get('file_name') is already in the DB. Not upserting"
                )
    
        if do_upsert:
            result = dic.upsert_file(file_info['filepath'], file_info['data_store'])
            log.info(f"upsert result: {result}")

            rows = dic.find_files(filenames = fname)
            for row in rows:
                log.info('Got a DB row')
                log.info(f"After: file_name: {row.get('file_name')}, location: {row.get('location')}, dir_path: {row.get('dir_path')}")

    except Exception as e:
        # Log the exception with full traceback and keep going
        if file_info['data_store']:
            data_store = file_info['data_store']
        else:
            data_store = "None"

        if file_info['filepath']:
            filepath = file_info['filepath']
        else:
            filepath = "None"
        
        msg = (
          f"Handling of file notification failed, data_store:"
          f" {data_store}, filepath: {filepath}"
        )
        log.exception(msg)

    log.info(" [x] Done")
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.info(" Done with 'ch.basic_ack'")


def connect_to_queue(config):
    """
    Establish or re-establish the connection and create a channel on that
    connection
    """
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config["Settings"]["RMQ_HOST"])
    )
    channel = connection.channel()

    # Ensure the durable task_queue exists
    channel.queue_declare(queue="file_notif_queue", durable=True)

    # Create the data inventory client object and allow it to be sent to the
    # rabbitmq callback
    dic = DIClient(user='geoips')
    bound_callback = partial(notif_callback, custom_object=dic)

    # Set up "whichever's ready" dispatching
    # Register the callback function with rabbitmq
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="file_notif_queue",
      on_message_callback=bound_callback)

    return channel

def consume_notification(config):
    """Get the notifications and add the files to the DB"""
    channel = connect_to_queue(config)

    # Start the "reconnection on error" loop
    while True:
        # Start the message checking loop
        log.info(" [*] Waiting for messages. To exit press CTRL+C")
        try:
            channel.start_consuming()
        except (
            OSError,
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.StreamLostError
        ) as e:
            log.exception(e)
            log.info("Reconnecting to RabbitMQ")
            channel = connect_to_queue(config)


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
