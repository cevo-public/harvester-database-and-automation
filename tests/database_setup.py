"""
Creates local test database from dump of production data.

The database dump is expected to be in the file created by
`database_dump.py`. The database name and connection details will
be read from the `test` section in `database.yaml`.
"""

from fixtures import database_config
from subprocess import run
from pathlib import Path
import os


if __name__ == '__main__':

    # Load configuration.
    (name, host, port, user, password) = database_config('test')

    # Check that database has already been dumped.
    file = (Path(__file__).parent/'database_dump.bak').relative_to(Path.cwd())
    if not file.exists():
        raise FileNotFoundError(f'Database dump file "{file}" does not exist.')

    # Delete existing test database and recreate from dump.
    command = ['psql']
    if host:
        command.append(f'--host={host}')
    if port:
        command.append(f'--port={port}')
    if user:
        command.append(f'--username={user}')
    if password:
        os.environ['PGPASSWORD'] = password
    run(command + [f'--command=drop database if exists {name};'], check=True)
    run(command + [f'--command=create database {name};'], check=True)
    run(command + [f'--dbname={name}', f'--file={file}'], check=True)
