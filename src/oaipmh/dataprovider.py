import logging
import os
from dataclasses import MISSING
from datetime import datetime
from typing import Optional, Any

from lxml import etree
# noinspection PyProtectedMember
from lxml.etree import _Element
from oai_repo import MetadataFormat, DataInterface, Identify, RecordHeader, Set, OAIRepoExternalException
from oai_repo.exceptions import OAIErrorCannotDisseminateFormat
from oai_repo.helpers import granularity_format
from requests import Session
from requests_jwtauth import HTTPBearerAuth

from oaipmh.oai import OAIIdentifier
from oaipmh.solr import Index
from oaipmh.transformers import load_transformers

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class EnvAttribute:
    """
    Descriptor class that maps an attribute of a class to an environment variable.
    """
    def __init__(self, env_var: str, default: Optional[Any] = MISSING):
        self.env_var = env_var
        self.default = default

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, instance, owner):
        if self.default is not MISSING:
            value = os.environ.get(self.env_var, self.default)
        else:
            value = os.environ.get(self.env_var)

        # if this attribute has a type annotation, use it to cast the
        # string value from the environment variable to some other type
        if getattr(instance, '__annotations__', False) and self.name in instance.__annotations__:
            return instance.__annotations__[self.name](value)
        else:
            return value


class DataProvider(DataInterface):
    admin_email = EnvAttribute('ADMIN_EMAIL')
    base_url = EnvAttribute('BASE_URL', 'http://localhost:5000/')
    datestamp_granularity = EnvAttribute('DATESTAMP_GRANULARITY', 'YYYY-MM-DDThh:mm:ssZ')
    earliest_datestamp = EnvAttribute('EARLIEST_DATESTAMP')
    oai_repository_name = EnvAttribute('OAI_REPOSITORY_NAME')
    oai_namespace_identifier = EnvAttribute('OAI_NAMESPACE_IDENTIFIER')
    report_deleted_records = EnvAttribute('REPORT_DELETED_RECORDS', 'no')
    limit: int = EnvAttribute('PAGE_SIZE', 25)

    def __init__(self, index: Index):
        self.index = index
        self.session = Session()
        self.session.auth = HTTPBearerAuth(os.environ.get('FCREPO_JWT_TOKEN'))
        self._transformers = load_transformers()

    def get_oai_identifier(self, handle: str) -> OAIIdentifier:
        """
        Given a handle, return a full OAI identifier.

        :param handle: resource handle, e.g. "1903.1/12345"
        :return: OAI Identifier
        """
        return OAIIdentifier(
            namespace_identifier=self.oai_namespace_identifier,
            local_identifier=handle,
        )

    def get_uri(self, identifier: str) -> str:
        """
        Given an OAI identifier string, return the URI for the fcrepo resource.

        :param identifier: OAI identifier string ("oai:...")
        :return: URI string
        """
        handle = OAIIdentifier.parse(identifier).local_identifier
        return self.index.get_doc(handle)[self.index.uri_field]

    def get_last_modified(self, identifier: str) -> datetime:
        """
        Given an OAI identifier string, return the last modified time for the fcrepo resource.

        :param identifier: OAI identifier string ("oai:...")
        :return: datetime object
        """
        handle = OAIIdentifier.parse(identifier).local_identifier
        last_modified = self.index.get_doc(handle)[self.index.last_modified_field]
        return datetime.fromisoformat(last_modified)

    def transform(self, target_format: str, xml_root: _Element) -> _Element:
        """
        Perform an XSLT transformation on the given XML element to the
        specified target format. The target formats are the same as the
        metadata prefixes.

        :param target_format: metadata prefix of the desired format
        :param xml_root: XML element to transform
        :return: XML element
        :raises OAIErrorCannotDisseminateFormat: if the target_format
        is not supported
        """
        try:
            transform = self._transformers[target_format]
        except KeyError:
            raise OAIErrorCannotDisseminateFormat
        return transform(xml_root)

    def get_identify(self) -> Identify:
        return Identify(
            base_url=self.base_url,
            admin_email=[self.admin_email],
            repository_name=self.oai_repository_name,
            earliest_datestamp=self.earliest_datestamp,
            deleted_record=self.report_deleted_records,
            granularity=self.datestamp_granularity,
        )

    def is_valid_identifier(self, identifier: str) -> bool:
        return identifier.startswith(f'oai:{self.oai_namespace_identifier}:')

    def get_metadata_formats(self, identifier: str | None = None) -> list[MetadataFormat]:
        return [transformer.metadata_format for transformer in self._transformers.values()]

    def get_record_header(self, identifier: str) -> RecordHeader:
        last_modified = self.get_last_modified(identifier)
        oai_id = OAIIdentifier.parse(identifier)
        handle = oai_id.local_identifier
        return RecordHeader(
            identifier=identifier,
            datestamp=granularity_format(self.datestamp_granularity, last_modified),
            setspecs=self.index.get_sets_for_handle(handle),
        )

    def get_record_metadata(self, identifier: str, metadataprefix: str) -> _Element | None:
        uri = self.get_uri(identifier)
        response = self.session.get(uri, headers={'Accept': 'application/rdf+xml'})
        if response.ok:
            rdf_xml = etree.fromstring(response.text)
            return self.transform(metadataprefix, rdf_xml)
        else:
            logger.error(f'GET {uri} -> {response.status_code} {response.reason}')
            raise OAIRepoExternalException('Unable to retrieve resource from fcrepo')

    def get_record_abouts(self, identifier: str) -> list[_Element]:
        return []

    def list_set_specs(self, identifier: str = None, cursor: int = 0) -> tuple:
        if identifier:
            oai_id = OAIIdentifier.parse(identifier)
            set_specs = self.index.get_sets_for_handle(oai_id.local_identifier).keys()
        else:
            set_specs = self.index.get_sets().keys()
        return set_specs, len(set_specs), None

    def get_set(self, setspec: str) -> Set:
        set_conf = self.index.get_set(setspec)
        return Set(spec=set_conf['spec'], name=set_conf['name'], description=[])

    def list_identifiers(
            self,
            metadataprefix: str,
            filter_from: datetime = None,
            filter_until: datetime = None,
            filter_set: str = None,
            cursor: int = 0
    ) -> tuple:
        logger.debug(
            'list_identifiers('
            f'metadataprefix={metadataprefix}, '
            f'filter_from={filter_from}, '
            f'filter_until={filter_until}, '
            f'filter_set={filter_set}, '
            f'cursor={cursor})'
        )
        results = self.index.get_docs(filter_from, filter_until, filter_set, start=cursor, rows=self.limit)
        identifiers = [str(self.get_oai_identifier(doc[self.index.handle_field])) for doc in results]
        return identifiers, results.hits, None
