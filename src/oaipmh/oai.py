import re
import urllib.parse


def get_set_spec(name: str) -> str:
    return re.sub('[^a-z0-9]+', '_', name.lower())


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
