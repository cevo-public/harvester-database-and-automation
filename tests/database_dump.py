"""
Dumps tables used in test suite from production database.

The script exports selected tables from the production database and
stores them in a file named the same as the script, but with a `.bak`
extension. The database connection details will be read from the
`production` section in `database.yaml`.

The table selection is hard-coded below. It is tailored to the tests
that will be run. The fewer tables there are, the quicker it is to
dump the database and recreate it locally.
"""

from fixtures import database_config
from subprocess import run
from pathlib import Path
import os


tables   = [
    'consensus_sequence',
    'consensus_sequence_meta',
    "consensus_sequence_mutation_aa",
    "consensus_sequence_mutation_nucleotide",
    "extraction_plate",
    'imv_metadata',
    'sequence_identifier',
    'sequencing_plate',
    'test_metadata',
    'test_plate_mapping',
]


if __name__ == '__main__':

    # Load configuration.
    (name, host, port, user, password) = database_config('production')

    # Dump selected tables from production database.
    command = ['pg_dump']
    command.append(f'--dbname={name}')
    if host:
        command.append(f'--host={host}')
    if port:
        command.append(f'--port={port}')
    if user:
        command.append(f'--username={user}')
    if password:
        os.environ['PGPASSWORD'] = password
    file = Path(__file__).with_suffix('.bak')
    command.append(f'--file={file}')
    for table in tables:
        command.append(f'--table={table}')
    run(command, check=True)
