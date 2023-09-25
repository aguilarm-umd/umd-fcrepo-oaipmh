# add-handles

Handles script for oaipmh

## Purpose

This script is supposed to create handles for items exported from Fedora that don't have handles.
These handles are minted via UMD-Handle, and are written out to a csv file.
The resulting csv file will be used in the plastron import command, which will update the items in Fedora.

## Development Environment

Same as in [README.md]

## Installation

Same as in [README.md]

### Configuration

Create a `handle_conf.yml` file with the following contents:

```yaml
# Endpoint for the handles server
# (e.g., http://localhost:3000/api/v1)
HANDLE_URL:
# The base URL for the items
# (e.g., http://fcrepo-local:8080/fcrepo/rest/)
BASE_URL:
# The public URL for where the items are in the frontend
# (e.g., https://digital.lib.umd.edu/result/id/)
PUBLIC_BASE_URL:
# The JWT authentication token generated from UMD-Handle
AUTH:

```

### Running

```zsh
$ add-handles -h
Usage: add-handles [OPTIONS]

Options:
  -i, --input-file FILENAME   The CSV export file to take in and add handles
                              to. Defaults to STDIN.
  -o, --output-file FILENAME  Output file for CSV file with handles. Defaults
                              to STDOUT.
  -c, --config-file FILENAME  Config file to use for interacting with UMD-
                              Handle and Fcrepo  [required]
  -V, --version               Show the version and exit.
  -h, --help                  Show this message and exit.
```

### Testing

Same as in [README.md]

[README.md]: ../README.md
