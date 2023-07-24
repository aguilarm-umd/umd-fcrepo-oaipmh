FROM python:3.11.2-slim

EXPOSE 5000

WORKDIR /opt/umd-fcrepo-oaipmh

# need git for the requests-jwtauth package to be installed from GitHub
RUN apt-get update && apt-get install -y git && apt-get clean

COPY requirements.txt /opt/umd-fcrepo-oaipmh/
RUN pip install -r requirements.txt
COPY src pyproject.toml /opt/umd-fcrepo-oaipmh/
RUN pip install -e .

ENTRYPOINT ["fcrepo-oaipmh-server"]
