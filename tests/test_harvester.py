import pytest
from sickle import Sickle


sickle = Sickle("http://localhost:5000/oai/api")


def test_identity():
    identity = sickle.Identify()
    assert identity.repositoryName == 'UMD Libraries'
    assert identity.baseURL == 'http://localhost:5000/oai/api'
    assert identity.protocolVersion == '2.0'
    assert identity.adminEmail == 'test@umd.edu'
    assert identity.earliestDatestamp == '2014-01-01'
    assert identity.granularity == 'YYYY-MM-DD'


def test_metadata_formats():
    metadata_formats = sickle.ListMetadataFormats()
    assert sum(1 for _ in metadata_formats) == 2

    # Used up the iterator so calling it again
    metadata_formats = sickle.ListMetadataFormats()

    for format in metadata_formats:
        assert format.metadataPrefix == 'rdf' or format.metadataPrefix == 'oai_dc'
        assert format.metadataNamespace == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#' or \
               format.metadataNamespace == 'http://www.openarchives.org/OAI/2.0/oai_dc/'
        assert format.schema == 'http://www.openarchives.org/OAI/2.0/rdf.xsd' or \
               format.schema == 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd'
