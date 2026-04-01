## new_file_notification

## Description
Uses RabbitMQ to receive new file notifications, with the ultimate purpose of
getting the file metadata into the Data Inventory Database.

The new file notifications are sent by the `ygd` command installed with the
`youvegotdata` PyPi module: `pip install youvegotdata`. This command is the
"producer." It is usually called by the CIRA data ingest scripts when a new
file is added to the CIRA data stores, and will send a message through RabbitMQ
to this project's consumers with the file's metadata.

The "consumer" `get_file_notif.py` is run in a container started up by this
project. The container will be created by the GeoIPS governance system to
receive the file metadata and insert it into the database. It is expected that
multiple containers will be created to accepting messages in RabbitMQ's "fair
dispatch" configuration. A given notification will be received by one
consumer.

## Running a consumer (receiver)
The consumer is meant to be run in a Docker container using docker compose. The
Docker image includes the needed Python environment.

Copy the template-config.ini file to config.ini and edit the config.ini as
described inside that file.

If the image needs to be built, run:
```
docker compose build
```
Run the consumer with:
```
docker compose up [-d]
```
The `-d` with run it detached from the terminal.

This will start up a persistent process that will consume the new file
notifications and use them to add the files metadata to the DB.

If it is run in a detached state a `docker compose down` will stop it. If not,
terminate it with Ctrl-C, wait ~10 seconds for it to stop, and then run
`docker compose down`.
