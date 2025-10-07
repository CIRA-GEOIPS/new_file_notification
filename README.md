## new_file_notification

## Description
Uses RabbitMQ to send and receive new file notifications, with the ultimate
purpose of getting the file metadata into the Data Inventory Database.

The "producer" `file_notification.py` will usually be called by the CIRA data
ingest scripts when a new file is added to the CIRA data stores, and will send
a message through RabbitMQ to the consumers with the file's metadata.

The "consumer" `get_file_notif.py` will be called by the GeoIPS governance
system to receive the file metadata and insert it into the database. It is
expected that multiple consumers process will be accepting messages in
RabbitMQ's "fair dispatch" configuration. A given notification will be received
by one consumer.

## Running the producer
This must be run in a Python environment that includes `pika` - for connecting
to RabbitMQ -  and other needed packages. The `environ-3.8.yml` file in this
repository can be used to create a workable conda environment. Setting one up
using `pip` will certainly also work. Python 3.8 is the minimum version needed
to run the script. Higher versions should work.

Copy the template-config.ini file to config.ini and edit the config.ini as
described inside that file.
Run the code with:
```
python file_notification.py [-h] [-v] [-s START_TIME] [-e END_TIME] [-l LENGTH] [-c CHECKSUM] [-t CHECKSUM_TYPE] filepath product version
```
Run this with the -h (--help) argument to see the available flagged arguments.

## Running a consumer
Get the config.ini file created and filled in as described above.

Run the code with:
```
python get_file_notif.py [&]
```
This will start up a persistent process that will consume the new file
notifications.

Run this with -h (--help) to print out its usage description and then exit.
Run it with -v (--verbose) to run it at the DEBUG level.
