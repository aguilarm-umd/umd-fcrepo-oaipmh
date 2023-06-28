import logging

import click
from dotenv import load_dotenv
from waitress import serve

from oaipmh import __version__
from oaipmh.web import create_app

load_dotenv()
logger = logging.getLogger(__name__)


@click.command()
@click.option(
    '--listen',
    default='0.0.0.0:5000',
    help='Address and port to listen on. Default is "0.0.0.0:5000".',
    metavar='[ADDRESS]:PORT',
)
@click.option(
    '--solr-config', 'solr_config_file',
    type=click.File(),
    help='Configuration file for the Solr index.',
)
@click.version_option(__version__, '--version', '-V')
@click.help_option('--help', '-h')
def run(listen, solr_config_file):
    server_identity = f'umd-fcrepo-oaipmh/{__version__}'
    logger.info(f'Starting {server_identity}')
    try:
        app = create_app(solr_config_file=solr_config_file)
        serve(app, listen=listen, ident=server_identity)
    except (OSError, RuntimeError) as e:
        logger.error(f'Exiting: {e}')
        raise SystemExit(1) from e
