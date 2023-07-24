import logging
import re
import sys
from csv import DictReader, DictWriter
from typing import TextIO

import click
import requests
import yaml

from oaipmh import __version__

logger = logging.getLogger(__name__)


class RequestFailure(Exception):
    pass


@click.command()
@click.option(
    '--input-file', '-i',
    help='The CSV export file to take in and add handles to. Defaults to STDIN.',
    default=sys.stdin,
    type=click.File(),
)
@click.option(
    '--output-file', '-o',
    help='Output file for CSV file with handles. Defaults to STDOUT.',
    default=sys.stdout,
    type=click.File(mode='w'),
)
@click.option(
    '--config-file', '-c',
    help='Config file to use for interacting with UMD-Handle and Fcrepo',
    type=click.File(mode='r'),
    required=True
)
@click.version_option(__version__, '--version', '-V')
@click.help_option('--help', '-h')
def main(input_file: TextIO, output_file: TextIO, config_file: TextIO):
    try:
        config = yaml.safe_load(config_file)
        reader = DictReader(input_file)
        exports = create_handles(reader, config)

        # ensure that there is a Handle header in the output file
        fieldnames = list(reader.fieldnames)
        if 'Handle' not in fieldnames:
            fieldnames.append('Handle')

        # Write out to file or stdout
        writer = DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in exports:
            writer.writerow(row)
    except RequestFailure as e:
        logger.error(str(e))


def create_handles(reader: DictReader, config: dict) -> list:
    exports = []
    for row in reader:
        if 'Handle' not in row or row['Handle'] is None:
            if row['URI'].startswith(config['BASE_URL']) is False:
                logger.warning(f"The URI ({row['URI']}) does not start with the BASE_URL ({config['BASE_URL']})")
                logger.warning("Skipping creating a handle for this URI")
                exports.append(row)
                continue

            fcrepo_path = row['URI'].replace(config['BASE_URL'], '')

            # extract relpath and uuid from fcrepo path
            relpath, item_uuid = extract_from_path(fcrepo_path)
            relpath = relpath.replace('/', ':')

            # create url for http request
            public_url = config['PUBLIC_BASE_URL'] + item_uuid + f"?relpath={relpath}"

            # Send http Request to Umd-Handle-API to mint handle
            handle_url = mint_handle(config,
                                     prefix='1903.1',
                                     url=public_url,
                                     repo='fcrepo',
                                     repo_id=fcrepo_path
                                     )

            handle = extract_prefix_suffix(handle_url=handle_url)

            # Store handle
            row['Handle'] = handle
            exports.append(row)
        else:
            exports.append(row)

    return exports


PATH_PATTERN = re.compile(r'(.*?)/(..)/(..)/(..)/(..)/(\2\3\4\5-....-....-....-............)')


def extract_from_path(fcrepo_path: str) -> tuple[str, str]:
    matches = PATH_PATTERN.match(fcrepo_path)
    if not matches:
        raise RuntimeError(f'Unable to extract relpath and uuid from {fcrepo_path}')
    relpath = matches[1]
    item_uuid = matches[6]
    return relpath, item_uuid


def extract_prefix_suffix(handle_url: str) -> str:
    url_pattern = r'^(http|https):\/\/.[^\/]*\/'

    return re.sub(url_pattern, 'hdl:', handle_url)


def mint_handle(config: dict, **json) -> str:
    endpoint = '/handles'
    headers = {'Authorization': f'Bearer {config["AUTH"]}'}
    response = requests.post(config['HANDLE_URL'] + endpoint, json=json, headers=headers)

    if response.status_code != 200:
        raise RequestFailure(f"Got a {response.status_code} error code, check the configuration file.")

    handle_url = response.json().get('handle_url')

    return handle_url
