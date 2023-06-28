# umd-fcrepo-oaipmh

OAI-PMH Server for Fedora

## Purpose

This is an [OAI-PMH] server for publishing metadata records from a Fedora 
repository. It uses a Solr index for general queries, and connects 
directly to Fedora to retrieve the full metadata record for an item.

## Development Environment

Python version: 3.11

### Installation

```bash
git clone git@github.com:umd-lib/umd-fcrepo-oaipmh.git
cd umd-fcrepo-oaipmh
pyenv install --skip-existing $(cat .python-version)
python -m venv .venv --prompt umd-fcrepo-oaipmh-py$(cat .python-version)
pip install -r test.requirements.txt -e .
```

### Configuration

Create a `.env` file with the following contents:

```bash
# OAI-PMH repository administrator email address
ADMIN_EMAIL=...
# domain name for the target Fedora repo
OAI_NAMESPACE_IDENTIFIER=...
# OAI-PMH repository name
OAI_REPOSITORY_NAME=...
# earliest datestamp of items in this repository
EARLIEST_DATESTAMP=2014-01-01T00:00:00Z
# JWT bearer token for accessing the Fedora repository
FCREPO_JWT_TOKEN=...
# URL to the Solr core to search
SOLR_URL=...
# enable debugging and hot reloading when run via "flask run"
FLASK_DEBUG=1
```

For full configuration information, see
[Configuration](docs/configuration.md).

### Running

To run the application in debug mode, with hot code reloading:

```bash
flask --app oaipmh.web:create_app run
```

The application will be available at <http://localhost:5000/>

To change the port, add a `BASE_URL` environment variable to the `.env` file:

```bash
# set when using a URL and/or port other than 
# the defaults ("localhost" and "5000")
BASE_URL=http://localhost:8000/
```

And add `-p {port number}` to the `flask` command:

```bash
# for example, to run on port 8000
flask --app oaipmh.web:create_app run -p 8000
```

### Testing

This project uses the [pytest] testing framework. To run the full
[test suite](tests):

```bash
pytest
```

To run the test suite with coverage information from [pytest-cov]:

```bash
pytest --cov src --cov-report term-missing
```

This project also uses [pycodestyle] as a style checker and linter:

```bash
pycodestyle src
```

Configuration of pycodestyle is found in the [tox.ini](tox.ini) file.

### Deploying using Docker

Build the image:

```bash
docker build -t docker.lib.umd.edu/fcrepo-oaipmh:latest .
```

If you need to build for multiple architectures (e.g., AMD and ARM), you 
can use `docker buildx`. This assumes you have a builder named "local" 
configured for use with your docker buildx system, and you are logged in 
to a Docker repository that you can push images to:

```bash
docker buildx build --builder local --platform linux/amd64,linux/arm64 \
    -t docker.lib.umd.edu/fcrepo-oaipmh:latest --push .
    
# then pull the image so it is available locally
docker pull docker.lib.umd.edu/fcrepo-oaipmh:latest
```

Run the container:

```bash
docker run -d -p 5000:5000 \
    -e ADMIN_EMAIL=... \
    -e OAI_NAMESPACE_IDENTIFIER=... \
    -e OAI_REPOSITORY_NAME=... \
    -e EARLIEST_DATESTAMP=2014-01-01T00:00:00Z \
    -e FCREPO_JWT_TOKEN=... \
    -e SOLR_URL=... \
    docker.lib.umd.edu/fcrepo-oaipmh:latest
```

If you created a `.env` file (see [Configuration](#configuration)), you 
can run the Docker image using that file.

```bash
docker run -d -p 5000:5000 \
    --env-file .env \
    docker.lib.umd.edu/fcrepo-oaipmh:latest
```

Note: To refer to services running on the host machine (e.g., Solr) in the 
configuration, you will need to use the hostname `host.docker.internal`
instead of `localhost`.

[OAI-PMH]: https://www.openarchives.org/pmh/
[pytest]: https://docs.pytest.org/en/7.3.x/
[pytest-cov]: https://pypi.org/project/pytest-cov/
[pycodestyle]: https://pycodestyle.pycqa.org/en/latest/
