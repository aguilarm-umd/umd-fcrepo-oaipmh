from oaipmh.utils import EnvAttribute


def test_descriptor(monkeypatch):
    monkeypatch.setenv('APP_NAME', 'foo')

    class Simple:
        name = EnvAttribute('APP_NAME')

    obj = Simple()
    assert obj.name == 'foo'


def test_descriptor_with_type(monkeypatch):
    monkeypatch.setenv('APP_LIMIT', '25')

    class Simple:
        limit: int = EnvAttribute('APP_LIMIT')

    obj = Simple()
    assert obj.limit == 25


def test_descriptor_with_default(monkeypatch):
    class Simple:
        title = EnvAttribute('THIS_VAR_DOES_NOT_EXIST', '[Untitled]')

    obj = Simple()
    assert obj.title == '[Untitled]'

    # switch to the env var value
    monkeypatch.setenv('THIS_VAR_DOES_NOT_EXIST', 'Hello World!')
    assert obj.title == 'Hello World!'


def test_descriptor_with_type_and_default(monkeypatch):
    class Simple:
        size: int = EnvAttribute('THIS_VAR_DOES_NOT_EXIST', '1')

    obj = Simple()
    assert obj.size == 1

    # switch to the env var value
    monkeypatch.setenv('THIS_VAR_DOES_NOT_EXIST', '50')
    assert obj.size == 50
