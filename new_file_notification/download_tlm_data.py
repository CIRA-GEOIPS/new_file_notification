#!/usr/bin/env python
# coding: utf-8

# Stock modules
import os
import sys
import logging
import argparse
import configparser

import requests

DESCRIPTION = """
Downloads telemetry data files from the BCTM OPS telemetry API at
https://api.bctmops.com/tlm using the OpenAPI specification described at
https://docs.bctmops.com/apidocs/?url=https://api.bctmops.com/tlm/openapi.json
"""

DEFAULT_API_BASE_URL = "https://api.bctmops.com/tlm"

log = logging.getLogger(__name__)


def get_auth_headers(access_token):
    """Return HTTP headers with Bearer token authentication."""
    return {"Authorization": f"Bearer {access_token}"}


def list_files(
    access_token,
    api_base_url,
    start_time=None,
    end_time=None,
    satellite=None,
    product=None,
):
    """
    List available telemetry data files from the API.

    Parameters
    ----------
    access_token : str
        Bearer token for authentication.
    api_base_url : str
        Base URL of the telemetry API.
    start_time : str, optional
        Filter files with data starting after this time (ISO 8601 format).
    end_time : str, optional
        Filter files with data ending before this time (ISO 8601 format).
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

    url = f"{api_base_url}/files"
    log.info(f"Querying {url} with params: {params}")
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # Handle both a direct list and a paginated/wrapped response
    if isinstance(data, list):
        return data
    for key in ("items", "results", "files", "data"):
        if key in data:
            return data[key]
    log.warning(
        f"Unexpected API response structure from {url}; expected a list or a "
        f"dict with a known list key. Response keys: {list(data.keys()) if isinstance(data, dict) else type(data)}"
    )
    return []


def download_file(access_token, api_base_url, file_info, output_dir):
    """
    Download a single telemetry data file.

    Parameters
    ----------
    access_token : str
        Bearer token for authentication.
    api_base_url : str
        Base URL of the telemetry API.
    file_info : dict
        File metadata dict as returned by list_files().
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
        # Fall back to constructing the URL from the file ID
        file_id = file_info.get("id") or file_info.get("file_id")
        if not file_id:
            log.error(f"Cannot determine download URL or file ID from: {file_info}")
            return None
        download_url = f"{api_base_url}/files/{file_id}/download"

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
        if "filename=" in content_disposition:
            filename = content_disposition.split("filename=")[-1].strip().strip('"')
    if not filename:
        # Fall back to a name derived from the file ID so the result is unique
        file_id = file_info.get("id") or file_info.get("file_id")
        if file_id:
            filename = f"tlm_file_{file_id}"
        else:
            filename = f"tlm_file_{hash(download_url)}"
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
    start_time=None,
    end_time=None,
    satellite=None,
    product=None,
    output_dir=".",
):
    """
    List and download telemetry data files from the BCTM OPS API.

    Parameters
    ----------
    config : configparser.ConfigParser
        Configuration object.  Must include Settings.ACCESS_TOKEN and
        optionally Settings.TLM_API_URL (defaults to
        https://api.bctmops.com/tlm).
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

    files = list_files(
        access_token, api_base_url, start_time, end_time, satellite, product
    )
    log.info(f"Found {len(files)} file(s)")

    downloaded = []
    for file_info in files:
        try:
            path = download_file(access_token, api_base_url, file_info, output_dir)
            if path:
                downloaded.append(path)
        except requests.HTTPError as exc:
            log.error(f"Failed to download file {file_info}: {exc}")

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
        "-s",
        "--start_time",
        default=None,
        help="Start of time range to download (ISO 8601, e.g. 2024-01-01T00:00:00Z)",
    )

    parser.add_argument(
        "-e",
        "--end_time",
        default=None,
        help="End of time range to download (ISO 8601, e.g. 2024-01-02T00:00:00Z)",
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

    downloaded = download_tlm_files(
        config,
        pargs.start_time,
        pargs.end_time,
        pargs.satellite,
        pargs.product,
        pargs.output_dir,
    )

    log.info(f"Successfully downloaded {len(downloaded)} file(s)")
    for path in downloaded:
        log.info(f"  {path}")


if __name__ == "__main__":
    main()
