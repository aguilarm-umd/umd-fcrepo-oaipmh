from unittest.mock import MagicMock

import pysolr
import pytest


collect_ignore = ["test_harvester.py"]


@pytest.fixture
def mock_solr_client():
    mock_solr = MagicMock(spec=pysolr.Solr)
    mock_solr.url = 'http://localhost:8983/solr/fcrepo'
    return mock_solr
