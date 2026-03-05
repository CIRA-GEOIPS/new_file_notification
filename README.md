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

The "downloader" `download_tlm_data.py` queries the BCTM OPS telemetry API
(https://api.bctmops.com/tlm) collector interfaces and downloads telemetry data
files to a local directory. The API is documented at
https://docs.bctmops.com/apidocs/?url=https://api.bctmops.com/tlm/openapi.json

## Configuration
Copy `template-config.ini` to `config.ini` and edit the values as described
inside that file. The `ACCESS_TOKEN` setting is required by
`download_tlm_data.py`. The `TLM_API_URL` setting is optional and defaults to
`https://api.bctmops.com/tlm`.

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
python file_notification.py [-h] [-v] [-p PRODUCT] [-r VERSION] [-s START_TIME] [-e END_TIME] [-l LENGTH] [-c CHECKSUM] [-t CHECKSUM_TYPE] filepath
```
Run this with the -h (--help) argument to see the available flagged arguments.

This will usually be run with just the `filepath` argument. An example is:
```
python new_file_notification/file_notification.py /full/path/to/local/file/data_file.hdf
```
If run from a local repository of this project.

The `filepath` file must exist on the local machine.

## Running a consumer
Get the config.ini file created and filled in as described above.

The consumer is meant to be run in a Docker container using docker compose. The
Docker image includes the needed Python environment.

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

## Running the telemetry data downloader
Ensure `config.ini` contains a valid `ACCESS_TOKEN` under `[Settings]`.

The downloader uses the BCTM OPS API collector interfaces to list available
collectors and download telemetry files from them.

Run the downloader with:
```
python new_file_notification/download_tlm_data.py [-h] [-v] [--collector COLLECTOR] [-s START_TIME] [-e END_TIME] [--satellite SATELLITE] [-p PRODUCT] [-o OUTPUT_DIR] [--list-collectors]
```
Run this with the `-h` (`--help`) argument to see all available options.

Example — list all available collectors:
```
python new_file_notification/download_tlm_data.py --list-collectors
```

Example — download all available files to a `./tlm_data` directory:
```
python new_file_notification/download_tlm_data.py -o ./tlm_data
```

Example — download files for a specific time range from all collectors:
```
python new_file_notification/download_tlm_data.py \
    -s 2024-01-01T00:00:00Z \
    -e 2024-01-02T00:00:00Z \
    -o ./tlm_data
```

Example — download files from a specific collector:
```
python new_file_notification/download_tlm_data.py \
    --collector my-collector-id \
    -o ./tlm_data
```

The script queries `GET /collectors` to discover collectors (unless
`--collector` is given), then queries
`GET /collectors/{collector_id}/files` with any supplied filters, and
downloads each returned file to the output directory.
