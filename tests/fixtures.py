"""Fixtures used by the test suite."""

import yaml
import io
import sys
from pathlib import Path


def database_config(instance: str, file: Path = None) -> tuple:
    """
    Loads database connection details from the configuration file.

    `instance` can either be `'production'` or `'test'`.
    The configuration `file`, if not specified, defaults to
    `database.yaml` in this module's folder.

    Returns a tuple `(name, host, user, port, password)`, all of
    which may be `None` if the option is `NULL` in the YAML config
    file or not specified at all.
    """
    if not file:
        file = Path(__file__).parent / 'database.yaml'
    if not file.exists():
        raise FileNotFoundError(f'Database config file "{file}" not found.')
    with file.open(encoding='UTF-8-sig') as stream:
        try:
            config = yaml.load(stream, Loader=yaml.FullLoader)
        except yaml.YAMLError as error:
            raise RuntimeError('Syntax error in config file "{file}".')
    if instance not in config:
        raise ValueError(f'No section named "{instance}" in "{file}.')
    name     = config[instance].get('name')
    host     = config[instance].get('host')
    port     = config[instance].get('port')
    user     = config[instance].get('user')
    password = config[instance].get('password')
    return (name, host, port, user, password)


class capture_stdout:
    """Captures text written to `sys.stdout` in this context."""

    def __enter__(self):
        self.stdout = sys.stdout
        self.buffer = io.StringIO()
        sys.stdout = self.buffer
        return self

    def __exit__(self, type, value, traceback):
        sys.stdout = self.stdout

    def text(self):
        return self.buffer.getvalue()
