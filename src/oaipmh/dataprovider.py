import logging
import os
import re
import urllib.parse
from collections.abc import Iterable
from dataclasses import MISSING
from datetime import datetime
from typing import Optional, Any

import pysolr
from lxml import etree
# noinspection PyProtectedMember
from lxml.etree import _Element
from oai_repo import MetadataFormat, DataInterface, Identify, RecordHeader, Set, OAIRepoExternalException
from oai_repo.exceptions import OAIErrorCannotDisseminateFormat, OAIErrorBadArgument
from oai_repo.helpers import datestamp_long, granularity_format
from requests import Session
from requests_jwtauth import HTTPBearerAuth

from oaipmh.transformers import load_transformers

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

BASE_QUERY = 'rdf_type:pcdm\\:Object AND component:* NOT component:Page NOT component:Article AND handle:*'
SETS = {}


class OAIIdentifier:
    @classmethod
    def parse(cls, identifier: str):
        if not identifier.startswith('oai:'):
            raise ValueError('OAI identifier must start with "oai:"')
        _, namespace_identifier, local_identifier = identifier.split(':', maxsplit=2)
        return cls(
            namespace_identifier=namespace_identifier,
            local_identifier=urllib.parse.unquote(local_identifier)
        )

    def __init__(self, namespace_identifier: str, local_identifier: str):
        self.namespace_identifier = namespace_identifier
        self.local_identifier = local_identifier

    def __str__(self):
        return f'oai:{self.namespace_identifier}:{urllib.parse.quote(self.local_identifier)}'


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

    def __init__(self, solr_client: pysolr.Solr):
        self.solr = solr_client
        self.solr_url = self.solr.url
        self.solr_results: dict[str, dict[str, Any]] = {}
        self.session = Session()
        self.session.auth = HTTPBearerAuth(os.environ.get('FCREPO_JWT_TOKEN'))
        self._transformers = load_transformers()

    @property
    def sets(self) -> dict[str, Set]:
        return {s.spec: s for s in get_sets(get_collection_titles(self.solr))}

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

    def get_solr_doc(self, identifier: str) -> dict[str, Any]:
        oai_id = OAIIdentifier.parse(identifier)
        handle = oai_id.local_identifier
        results = self.solr.search(q=f'handle:{handle}')
        if not results:
            raise OAIRepoExternalException(f'Unable to find handle {handle} in Solr')
        doc = results.docs[0]
        # cache the resulting doc
        self.solr_results[str(oai_id)] = doc
        return doc

    def get_uri(self, identifier: str) -> str:
        """
        Given an OAI identifier string, return the URI for the fcrepo resource.

        :param identifier: OAI identifier string ("oai:...")
        :return: URI string
        :raises OAIRepoExternalException:
        """
        try:
            return self.solr_results[identifier]['id']
        except KeyError:
            # not found in the Solr results, fall back to a new Solr request
            return self.get_solr_doc(identifier)['id']

    def get_last_modified(self, identifier: str) -> datetime:
        try:
            last_modified = self.solr_results[identifier]['last_modified']
        except KeyError:
            # not found in the Solr results, fall back to a new Solr request
            last_modified = self.get_solr_doc(identifier)['last_modified']

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
        return RecordHeader(
            identifier=identifier,
            datestamp=granularity_format(self.datestamp_granularity, last_modified),
            # TODO: include setSpec elements
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
        return self.sets, len(self.sets), None

    def get_set(self, setspec: str) -> Set:
        return self.sets[setspec]

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
        filter_query = BASE_QUERY
        if filter_from or filter_until:
            datetime_range = get_solr_date_range(filter_from, filter_until)
            filter_query += f' AND last_modified:{datetime_range}'
        if filter_set:
            if filter_set in self.sets:
                filter_query += f' AND collection_title_facet:"{self.sets[filter_set].name}"'
            elif filter_set in SETS:
                filter_query += f' AND ({SETS[filter_set]["filter"]})'
            else:
                raise OAIErrorBadArgument(f"'{filter_set}' is not a valid setSpec value")
        logger.debug(f'Solr fq = "{filter_query}"')
        try:
            results = self.solr.search(q='*:*', fq=filter_query, start=cursor, rows=self.limit)
        except pysolr.SolrError as e:
            raise OAIRepoExternalException('Unable to connect to Solr') from e
        for doc in results:
            oai_id = self.get_oai_identifier(doc['handle'])
            self.solr_results[str(oai_id)] = doc
        identifiers = list(self.solr_results.keys())
        return identifiers, results.hits, None


def get_solr_date_range(timestamp_from: Optional[datetime], timestamp_until: Optional[datetime]) -> str:
    try:
        datestamp_from = datestamp_long(timestamp_from) if timestamp_from else '*'
        datestamp_until = datestamp_long(timestamp_until) if timestamp_until else '*'
    except AttributeError as e:
        raise TypeError("'timestamp_from' and 'timestamp_until', if present, must be datetime objects") from e
    return f'[{datestamp_from} TO {datestamp_until}]'


def get_set_spec(title: str) -> str:
    return re.sub('[^a-z0-9]+', '_', title.lower())


def get_sets(titles: Iterable[str]) -> list[Set]:
    return [Set(spec=get_set_spec(title), name=title, description=[]) for title in titles]


def get_collection_titles(solr: pysolr.Solr) -> list[str]:
    try:
        results = solr.search(q='component:Collection', fl='display_title')
    except pysolr.SolrError as e:
        raise OAIRepoExternalException('Unable to connect to Solr') from e
    return [doc['display_title'] for doc in results]
