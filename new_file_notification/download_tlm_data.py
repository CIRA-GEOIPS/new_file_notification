#!/usr/bin/env python
# coding: utf-8

# Stock modules
import hashlib
import os
import sys
import logging
import argparse
import configparser

import requests

DESCRIPTION = """
Downloads telemetry data files from the BCTM OPS telemetry API at
https://api.bctmops.com/tlm using the collector interfaces described at
https://docs.bctmops.com/apidocs/?url=https://api.bctmops.com/tlm/openapi.json

Authentication requires an ACCESS_TOKEN in config.ini [Settings].

Collector interfaces allow you to list available collectors and download the
telemetry files they have collected. Optionally filter by collector ID, time
range, satellite, or product.
"""

DEFAULT_API_BASE_URL = "https://api.bctmops.com/tlm"

log = logging.getLogger(__name__)


def get_auth_headers(access_token):
    """Return HTTP headers with Bearer token authentication."""
    return {"Authorization": f"Bearer {access_token}"}


def list_collectors(access_token, api_base_url):
    """
    List available telemetry data collectors from the API.

    Parameters
    ----------
    access_token : str
        Bearer token for authentication.
    api_base_url : str
        Base URL of the telemetry API.

    Returns
    -------
    list of dict
        List of collector metadata dictionaries.
    """
    headers = get_auth_headers(access_token)
    url = f"{api_base_url}/collectors"
    log.info(f"Querying {url}")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    data = response.json()
    if isinstance(data, list):
        return data
    for key in ("items", "results", "collectors", "data"):
        if key in data:
            return data[key]
    log.warning(
        f"Unexpected API response structure from {url}; expected a list or a "
        f"dict with a known list key. Response keys: "
        f"{list(data.keys()) if isinstance(data, dict) else type(data)}"
    )
    return []


def list_collector_files(
    access_token,
    api_base_url,
    collector_id,
    start_time=None,
    end_time=None,
    satellite=None,
    product=None,
):
    """
    List telemetry data files available from a specific collector.

    Parameters
    ----------
    access_token : str
        Bearer token for authentication.
    api_base_url : str
        Base URL of the telemetry API.
    collector_id : str or int
        Identifier of the collector to query.
    start_time : str, optional
        Filter files with data starting at or after this time (ISO 8601).
    end_time : str, optional
        Filter files with data ending at or before this time (ISO 8601).
    satellite : str, optional
        Filter by satellite identifier.
    product : str, optional
        Filter by product type.

    Returns
    -------
    list of dict
        List of file metadata dictionaries.
    """
    headers = get_auth_headers(access_token)
    params = {}
    if start_time:
        params["start_time"] = start_time
    if end_time:
        params["end_time"] = end_time
    if satellite:
        params["satellite"] = satellite
    if product:
        params["product"] = product

    url = f"{api_base_url}/collectors/{collector_id}/files"
    log.info(f"Querying {url} with params: {params}")
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    if isinstance(data, list):
        return data
    for key in ("items", "results", "files", "data"):
        if key in data:
            return data[key]
    log.warning(
        f"Unexpected API response structure from {url}; expected a list or a "
        f"dict with a known list key. Response keys: "
        f"{list(data.keys()) if isinstance(data, dict) else type(data)}"
    )
    return []


def download_collector_file(
    access_token, api_base_url, collector_id, file_info, output_dir
):
    """
    Download a single telemetry data file from a collector.

    Parameters
    ----------
    access_token : str
        Bearer token for authentication.
    api_base_url : str
        Base URL of the telemetry API.
    collector_id : str or int
        Identifier of the collector the file belongs to.
    file_info : dict
        File metadata dict as returned by list_collector_files().
    output_dir : str
        Local directory to save the downloaded file.

    Returns
    -------
    str or None
        Path to the downloaded file, or None on failure.
    """
    headers = get_auth_headers(access_token)

    # Prefer an explicit download URL if provided in the metadata
    download_url = file_info.get("download_url") or file_info.get("url")
    if not download_url:
        file_id = file_info.get("id") or file_info.get("file_id")
        if not file_id:
            log.error(f"Cannot determine download URL or file ID from: {file_info}")
            return None
        download_url = (
            f"{api_base_url}/collectors/{collector_id}/files/{file_id}/download"
        )

    log.info(f"Downloading from {download_url}")
    response = requests.get(
        download_url, headers=headers, stream=True, timeout=300
    )
    response.raise_for_status()

    # Determine the output filename
    filename = file_info.get("name") or file_info.get("filename")
    if not filename:
        # Try to get filename from Content-Disposition header
        content_disposition = response.headers.get("Content-Disposition", "")
        for part in content_disposition.split(";"):
            part = part.strip()
            if part.lower().startswith("filename="):
                filename = part[len("filename="):].strip().strip('"').strip("'")
                break
    if not filename:
        # Fall back to a name derived from the file ID so the result is unique
        file_id = file_info.get("id") or file_info.get("file_id")
        if file_id:
            filename = f"tlm_collector_{collector_id}_file_{file_id}"
        else:
            url_hash = hashlib.md5(download_url.encode()).hexdigest()[:8]
            filename = f"tlm_collector_{collector_id}_file_{url_hash}"
        log.warning(
            f"Could not determine filename from metadata or headers; "
            f"using fallback name '{filename}'"
        )

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)
    with open(output_path, "wb") as fobj:
        for chunk in response.iter_content(chunk_size=8192):
            fobj.write(chunk)

    log.info(f"Saved to {output_path}")
    return output_path


def download_tlm_files(
    config,
    collector_id=None,
    start_time=None,
    end_time=None,
    satellite=None,
    product=None,
    output_dir=".",
):
    """
    List and download telemetry data files from the BCTM OPS API collector
    interfaces.

    If no collector_id is specified, all available collectors are queried and
    files are downloaded from each one.

    Parameters
    ----------
    config : configparser.ConfigParser
        Configuration object.  Must include Settings.ACCESS_TOKEN and
        optionally Settings.TLM_API_URL (defaults to
        https://api.bctmops.com/tlm).
    collector_id : str or int, optional
        Restrict downloads to a single collector.  When None (default) all
        collectors are queried.
    start_time : str, optional
        Start of time range to download (ISO 8601).
    end_time : str, optional
        End of time range to download (ISO 8601).
    satellite : str, optional
        Satellite identifier filter.
    product : str, optional
        Product type filter.
    output_dir : str
        Directory in which to save downloaded files.

    Returns
    -------
    list of str
        Paths of the successfully downloaded files.
    """
    access_token = config["Settings"]["ACCESS_TOKEN"]
    api_base_url = config["Settings"].get("TLM_API_URL", DEFAULT_API_BASE_URL)
    log.info(f"Using API base URL: {api_base_url}")

    # Determine which collectors to query
    if collector_id is not None:
        collector_ids = [collector_id]
    else:
        collectors = list_collectors(access_token, api_base_url)
        log.info(f"Found {len(collectors)} collector(s)")
        collector_ids = [
            c.get("id") or c.get("collector_id") for c in collectors
        ]
        collector_ids = [cid for cid in collector_ids if cid is not None]

    downloaded = []
    for cid in collector_ids:
        log.info(f"Processing collector: {cid}")
        try:
            files = list_collector_files(
                access_token, api_base_url, cid, start_time, end_time,
                satellite, product
            )
        except requests.HTTPError as exc:
            log.error(f"Failed to list files for collector {cid}: {exc}")
            continue

        log.info(f"Collector {cid}: found {len(files)} file(s)")
        for file_info in files:
            try:
                path = download_collector_file(
                    access_token, api_base_url, cid, file_info, output_dir
                )
                if path:
                    downloaded.append(path)
            except requests.HTTPError as exc:
                log.error(
                    f"Failed to download file {file_info} from "
                    f"collector {cid}: {exc}"
                )

    return downloaded


def main():

    # Parse the arguments
    parser = argparse.ArgumentParser(description=DESCRIPTION)

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output - set log level to DEBUG",
    )

    parser.add_argument(
        "--collector",
        default=None,
        help=(
            "Collector ID to download from. When omitted, files are downloaded "
            "from all available collectors."
        ),
    )

    parser.add_argument(
        "-s",
        "--start_time",
        default=None,
        help=(
            "Start of time range to download "
            "(ISO 8601, e.g. 2024-01-01T00:00:00Z)"
        ),
    )

    parser.add_argument(
        "-e",
        "--end_time",
        default=None,
        help=(
            "End of time range to download "
            "(ISO 8601, e.g. 2024-01-02T00:00:00Z)"
        ),
    )

    parser.add_argument(
        "--satellite",
        default=None,
        help="Filter by satellite identifier",
    )

    parser.add_argument(
        "-p",
        "--product",
        default=None,
        help="Filter by product type",
    )

    parser.add_argument(
        "-o",
        "--output_dir",
        default=".",
        help="Directory to save downloaded files (default: current directory)",
    )

    parser.add_argument(
        "--list-collectors",
        action="store_true",
        help="List available collectors and exit without downloading files",
    )

    pargs = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s%(name)s: %(message)s",
        level="DEBUG" if pargs.verbose else "INFO",
    )

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read("config.ini")

    if not config.has_option("Settings", "ACCESS_TOKEN"):
        log.error(
            "ACCESS_TOKEN not found in config.ini [Settings]. "
            "Copy template-config.ini to config.ini and set your token."
        )
        sys.exit(1)

    access_token = config["Settings"]["ACCESS_TOKEN"]
    api_base_url = config["Settings"].get("TLM_API_URL", DEFAULT_API_BASE_URL)

    if pargs.list_collectors:
        collectors = list_collectors(access_token, api_base_url)
        log.info(f"Found {len(collectors)} collector(s):")
        for c in collectors:
            log.info(f"  {c}")
        return

    downloaded = download_tlm_files(
        config,
        collector_id=pargs.collector,
        start_time=pargs.start_time,
        end_time=pargs.end_time,
        satellite=pargs.satellite,
        product=pargs.product,
        output_dir=pargs.output_dir,
    )

    log.info(f"Successfully downloaded {len(downloaded)} file(s)")
    for path in downloaded:
        log.info(f"  {path}")


if __name__ == "__main__":
    main()
