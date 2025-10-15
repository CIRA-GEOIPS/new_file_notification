#!/usr/bin/env python
# coding: utf-8

# Stock modules
import os
import sys
import logging
import argparse
import pika
import json
import configparser
import shlex

DESCRIPTION = """
Allows a data ingest process to send a new file notification to the GeoIPS
RabbitMQ server that accepts those kinds of notifications. The notification
will ultimately be used to add the file metadata to the GeoIPS Data Inventory
DB.
"""

log = logging.getLogger(__name__)

import shlex


def parse_mtab_alike(fobj):
    """
    Parses /etc/mtab (or /proc/self/mounts) and yields a dictionary for each entry.
    fobj: The handle for the alread opened file to parse
    """
    for line in fobj:
        # Ignore blank lines and comments (though mtab does not typically have them)
        if not line.strip() or line.strip().startswith("#"):
            continue

        # Use shlex to correctly split fields, respecting quotes
        fields = shlex.split(line)

        # Skip malformed lines
        if len(fields) < 6:
            continue

        yield {
            "device": fields[0],
            "mount_point": fields[1],
            "fs_type": fields[2],
            "options": fields[3],
            "dump_freq": fields[4],
            "pass_num": fields[5],
        }


def parse_mtab():
    """
    Parses /etc/mtab (or /proc/self/mounts) and yields a dictionary for each entry.
    """
    try:
        # with open("/etc/mtab", "r") as fobj:
        #    generator_object = parse_mtab_alike(fobj)
        fobj = open("/etc/mtab", "r")
        generator_object = parse_mtab_alike(fobj)
    except FileNotFoundError:
        log.warning("/etc/mtab not found. Trying /proc/self/mounts.")
        try:
            # with open("/proc/self/mounts", "r") as fobj:
            #    generator_object = parse_mtab_alike(fobj)
            fobj = open("/proc/self/mounts", "r")
            generator_object = parse_mtab_alike(fobj)
        except FileNotFoundError:
            log.error("Could not open /proc/self/mounts.")

    for mount in generator_object:
        yield mount


def resolve_data_store(filepath):
    """
    Get the data store name and the absolute path from the data store.
    filepath: The filepath argument given to the program
    """
    # Read the /etc/mtab file to get the data store and mount point
    # Example usage
    data_store = None
    fpath = None
    mp_match_len = 0
    log.info("Currently mounted filesystems:")
    for mount in parse_mtab():
        log.debug(
            f"Device: {mount['device']:<20} Mount Point: {mount['mount_point']:<20} FS Type: {mount['fs_type']:<10} Options: {mount['options']}"
        )
        if mount["mount_point"] == "/":
            # Skip this - every path will match it
            continue
        # Check all the mount points and use the one with the longest match
        if (
            filepath.startswith(mount["mount_point"])
            and len(mount["mount_point"]) > mp_match_len
        ):
            mp_match_len = len(mount["mount_point"])
            dev_dir = mount["device"].split(":")
            data_store = dev_dir[0]
            if len(dev_dir) == 2:
                # There is a path associated with the device. Replace the mount point with this path.
                fpath = dev_dir[1] + filepath[len(mount["mount_point"]) :]
            else:
                fpath = filepath

    return data_store, fpath


def produce_notification(
    config,
    filepath,
    product,
    version,
    start_time=None,
    end_time=None,
    length=None,
    checksum=None,
    checksum_type=None,
):
    """
    Send a "Fair Dispatch" message via RabbitMQ
    """

    # Get the data store name and the absolute path from the data store
    data_store, fpath = resolve_data_store(filepath)
    log.info(f"data_store: {data_store}, fpath: {fpath}")

    log.info(f'RMQ_HOST:  {config["Settings"]["RMQ_HOST"]}')

    # Establish connection and create a channel on that connection
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=config["Settings"]["RMQ_HOST"])
    )
    channel = connection.channel()

    # Ensure the durable file_notif_queue exists
    channel.queue_declare(queue="file_notif_queue", durable=True)

    # Put the message data in a dictionary for conversion to JSON
    msg_dict = {
        "data_store": data_store,
        "filepath": fpath,
        "product": product,
        "version": version,
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
        "-p",
        "--product",
        default=None,
        help="The file's product",
    )

    parser.add_argument(
        "-r",
        "--version",
        default=None,
        help="The file's version",
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

    # Reduce pika logging
    logging.getLogger("pika").setLevel(logging.WARNING)

    # Read the configuration file
    config = configparser.ConfigParser()
    try:
        config.read("config.ini")
    except FileNotFoundError:
        log.error("config.ini not found. Please ensure the file exists.")
        exit()

    log.info("Sending new file notification to GeoIPS")

    produce_notification(
        config,
        pargs.filepath,
        pargs.product,
        pargs.version,
        pargs.start_time,
        pargs.end_time,
        pargs.length,
        pargs.checksum,
        pargs.checksum_type,
    )


if __name__ == "__main__":
    main()
