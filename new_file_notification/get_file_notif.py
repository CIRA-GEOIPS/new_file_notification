#!/usr/bin/env python
import sys, os
import logging
import argparse
import pika
import json
import configparser
from functools import partial

# GeoIPS modules: the data inventory client
import data_inv_api.pg_di_client as diapi
from data_inv_api import DIClient
from data_inv_api.errors import DIClientError, DIClientPgError

DESCRIPTION = """
Receives a new file notification from the GeoIPS RabbitMQ "New File
Notification" server, and adds the file metadata to the GeoIPS Data Inventory
DB.
"""

log = logging.getLogger(__name__)

def notif_callback(ch, method, properties, body, dic):
    """The receive message callback function

    Conforming messages about new files result in upsert to the
    inventory database. If the upsert succeeds or is a no-op, the
    message is ackowledged (ack).

    Non-conforming messages are rejected with no requeuing (nack), and
    the callback returns.  When database connections fail, messages are
    sent back and requeued (nack), and the callback returns.  On an
    unexpected error, the message is sent back and requeued (nack), and
    the callback reraises the exception.

    Parameters
    ----------
    ch :
        Pika channel for ack/nack.
    method :
        RabbitMQ method. Unused.
    properties:
        RabbitMQ properties. Unused.
    body: bytes
        The received message.
    dic: DIClient
        Data inventory client instance.

    """
    try:
        file_info = json.loads(body)
    except (TypeError, UnicodeDecodeError, json.decoder.JSONDecodeError):
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        log.exception(
            "Rejected message that was not bytes, not UTF, or not JSON. Message not requeued. "
            "ch=%r, method=%r, properties=%r, body=%r, dic=%r",
            ch,
            method,
            properties,
            body,
            dic,
        )
        return

    try:
        filepath = file_info["filepath"]
        _ = file_info["data_store"]
    except (TypeError, KeyError):
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        log.exception(
            "Rejected JSON message that doesn't conform to file info spec. Not requeued. "
            "ch=%r, method=%r, properties=%r, body=%r, dic=%r",
            ch,
            method,
            properties,
            body,
            dic,
        )
        return

    log.info(f" [x] Received file_info: {file_info}")
    do_upsert = True

    try:
        fname = os.path.basename(filepath)
        rows = dic.find_files(filenames=fname)

        for row in rows:
            log.info('Got a DB row')
            log.info(
                f"Before: file_name: {row.get('file_name')}, location:"
                f" {row.get('location')}, dir_path: {row.get('dir_path')},"
                f" size: {row.get('size')}"
            )
            db_fpath = os.path.join(row.get("dir_path"), row.get("file_name"))
            local_fpath = diapi.get_local_fpath(db_fpath, row.get("location"))
            curr_size = os.path.getsize(local_fpath)
            log.info(
                f"Before: local_fpath: {local_fpath}, curr_size: {curr_size}"
            )

            if (
                db_fpath == file_info['filepath'] and row.get('location') ==
                file_info['data_store'] and row.get('size') == curr_size
            ):
                log.info(
                    f"{row.get('file_name')} is already in the DB. Not"
                    f" upserting"
                )
                do_upsert = False

        if do_upsert:
            result = dic.upsert_file(
                file_info['filepath'], file_info['data_store'],
                file_info.get('product'), file_info.get('version'),
                file_info.get('platform_name'), file_info.get('source_name'),
                file_info.get('addl_metadata'), file_info.get('start_time'),
                file_info.get('end_time'), file_info.get('checksum'),
                file_info.get('size', file_info.get('length'))
            )
            log.info(f"upsert result: {result}")

            rows = dic.find_files(filenames = fname)
            for row in rows:
                log.info('Got a DB row')
                log.info(f"After: file_name: {row.get('file_name')}, location: {row.get('location')}, dir_path: {row.get('dir_path')}")

    except DIClientPgError:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        log.exception(
            "Database connection failed. Message requeued. "
            "ch=%r, method=%r, properties=%r, body=%r, dic=%r",
            ch,
            method,
            properties,
            body,
            dic,
        )
    except DIClientError:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        log.exception(
            "Rejected unprocessable message. Not requeued. "
            "ch=%r, method=%r, properties=%r, body=%r, dic=%r",
            ch,
            method,
            properties,
            body,
            dic,
        )
    except FileNotFoundError:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        log.exception(
            "Mount info not found. Message requeued. "
            "ch=%r, method=%r, properties=%r, body=%r, dic=%r",
            ch,
            method,
            properties,
            body,
            dic,
        )
    # Below we handle all exceptions not intentionally raised by the
    # data inventory client or callback itself.  Presumably these are
    # defects of this module or its dependencies and not infra failures,
    # requiring release and redeployment to fix.
    except:
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        log.exception(
            "File notification callback failed due to unexpected error. Message requeued."
            "ch=%r, method=%r, properties=%r, body=%r, dic=%r",
            ch,
            method,
            properties,
            body,
            dic,
        )
        raise
    else:
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
    bound_callback = partial(notif_callback, dic=dic)

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
