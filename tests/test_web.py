from unittest.mock import MagicMock

import pytest
from oai_repo.response import OAIResponse

from oaipmh.dataprovider import DataProvider
from oaipmh.solr import Index, DEFAULT_SOLR_CONFIG
from oaipmh.web import create_app, get_config, status


@pytest.fixture
def data_provider(monkeypatch, mock_solr_client, mock_solr_result):
    monkeypatch.setenv('ADMIN_EMAIL', 'peichman@umd.edu')
    monkeypatch.setenv('BASE_URL', 'http://localhost:5000/oai/api')
    monkeypatch.setenv('OAI_NAMESPACE_IDENTIFIER', 'fcrepo-local')
    monkeypatch.setenv('OAI_REPOSITORY_NAME', 'UMD Libraries')
    monkeypatch.setenv('DATESTAMP_GRANULARITY', 'YYYY-MM-DD')
    monkeypatch.setenv('EARLIEST_DATESTAMP', '2014-01-01')
    mock_solr_client.search = MagicMock(return_value=mock_solr_result)
    return DataProvider(
        Index(
            config=DEFAULT_SOLR_CONFIG,
            solr_client=mock_solr_client,
        )
    )


@pytest.fixture
def sample_config(datadir):
    return {
        'base_query': 'hdl:*',
        'handle_field': 'hdl',
        'uri_field': 'id',
        'last_modified_field': 'last_modified',
        'auto_create_sets': False,
        'sets': [],
    }


def test_status_ok():
    oai_response = MagicMock(spec=OAIResponse)
    oai_response.__bool__.return_value = True
    assert status(oai_response) == 200


def test_status_not_found():
    mock_error = MagicMock()
    mock_error.get.return_value = 'noRecordsMatch'
    oai_response = MagicMock(spec=OAIResponse)
    oai_response.__bool__.return_value = False
    oai_response.xpath.return_value = [mock_error]
    assert status(oai_response) == 404


def test_status_bad_request():
    mock_error = MagicMock()
    mock_error.get.return_value = 'otherError'
    oai_response = MagicMock(spec=OAIResponse)
    oai_response.__bool__.return_value = False
    oai_response.xpath.return_value = [mock_error]
    assert status(oai_response) == 400


def test_get_config_default():
    assert get_config(None) == DEFAULT_SOLR_CONFIG


def test_get_config_from_filename(datadir, sample_config):
    assert get_config(str(datadir / 'config.yml')) == sample_config


def test_get_config_from_file(datadir, sample_config):
    with (datadir / 'config.yml').open() as fh:
        assert get_config(fh) == sample_config


def test_home(data_provider):
    app = create_app(data_provider)
    app_client = app.test_client()
    response = app_client.get('/oai')
    assert response.status_code == 200


@pytest.mark.parametrize(
    ('http_method',),
    [['get'], ['post']],
)
def test_identify(http_method, data_provider):
    app = create_app(data_provider=data_provider)
    app_client = app.test_client()
    request = getattr(app_client, http_method)
    response = request('/oai/api?verb=Identify')
    assert response.status_code == 200


@pytest.mark.parametrize(
    ('http_method',),
    [['get'], ['post']],
)
def test_list_sets(http_method, data_provider):
    app = create_app(data_provider=data_provider)
    app_client = app.test_client()
    request = getattr(app_client, http_method)
    response = request('/oai/api?verb=ListSets')
    assert response.status_code == 200


@pytest.mark.parametrize(
    ('http_method',),
    [['get'], ['post']],
)
def test_list_metadata_formats(http_method, data_provider):
    app = create_app(data_provider=data_provider)
    app_client = app.test_client()
    request = getattr(app_client, http_method)
    response = request('/oai/api?verb=ListMetadataFormats')
    assert response.status_code == 200


@pytest.mark.parametrize(
    ('http_method',),
    [['get'], ['post']],
)
def test_list_identifiers(http_method, data_provider):
    app = create_app(data_provider=data_provider)
    app_client = app.test_client()
    request = getattr(app_client, http_method)
    response = request('/oai/api?verb=ListIdentifiers&metadataPrefix=oai_dc')
    assert response.status_code == 200
