"""Tests the `import_sequences.py` script."""


########################################
# Imports                              #
########################################

import sys
from pathlib import Path
here = Path(__file__).parent
sys.path.insert(0, str(here.parent.resolve()/'database'/'python'))

import import_sequences

from fixtures import capture_stdout
from fixtures import database_config
from unittest import mock
import builtins
import psycopg2


########################################
# Setup                                #
########################################

connection = None

new_samples = [
    '38003696_CP336_A10_type_opt',
    'ETHID_CP336_G12_positiveControl1_MT007544_1',
    'ETHID_CP336_H12_negativeControl_H2O',
]


def delete_if_in_database(samples_names):
    cursor = connection.cursor()
    for sample_name in samples_names:
        for table in ('consensus_sequence', 'consensus_sequence_meta'):
            cursor.execute(f"select sample_name from {table} "
                           "where sample_name = %s", (sample_name,))
            if cursor.fetchone():
                cursor.execute(f"delete from {table} "
                               "where sample_name = %s", (sample_name,))
                connection.commit()


def setup_module():
    global connection

    assert (here/'fixtures').exists()
    assert (here/'fixtures'/'sampleset').exists()
    assert (here/'fixtures'/'working').exists()
    assert (here/'fixtures'/'working'/'samples').exists()

    (name, host, port, user, password) = database_config('test')
    connection = psycopg2.connect(dbname=name, host=host, port=port, user=user,
                                  password=password, connect_timeout=3)

    with connection.cursor() as cursor:
        cursor.execute("select test_id from test_metadata "
                        "where test_id like '%/38003695';")
        assert cursor.fetchone()[0] == 'viollier/38003695'
        cursor.execute("select test_id from test_metadata "
                        "where test_id like '%/11fad1b9';")
        assert not cursor.fetchone()
        cursor.execute("select test_id from test_metadata "
                        "where test_id like '%/1784849c';")
        assert not cursor.fetchone()


########################################
# Tests                                #
########################################

def test_read_sample_names():
    sample_list = here/'fixtures'/'sampleset'/'samples.20220909_HJ5KWDRX2.tsv'
    assert sample_list.exists()
    sample_names = import_sequences.read_sample_names(sample_list)
    assert len(sample_names) == 6
    assert '11fad1b9_CP338_H3_type_opt' in sample_names
    assert '1784849c_CP338_G2_type_opt' in sample_names
    assert '38003695_CP336_A1_type_opt' in sample_names
    assert '38003696_CP336_A10_type_opt' in sample_names
    assert 'ETHID_CP336_G12_positiveControl1_MT007544_1' in sample_names
    assert 'ETHID_CP336_H12_negativeControl_H2O' in sample_names


def test_read_sequence():
    sample = here/'fixtures'/'working'/'samples'/'38003695_CP336_A1_type_opt'
    batch  = sample/'20220909_HJ5KWDRX2'
    file   = batch/'references'/'consensus_ambig.bcftools.fasta'
    assert sample.is_dir()
    assert batch.is_dir()
    assert file.is_file()
    sequence = import_sequences.read_sequence(file)
    assert len(sequence) == 29903


def test_import_sequences_dryrun():
    (name, host, _, user, password) = database_config('test')
    delete_if_in_database(new_samples)

    data_folder = here/'fixtures'/'working'/'samples'
    sample_list = here/'fixtures'/'sampleset'/'samples.20220909_HJ5KWDRX2.tsv'
    sample_names = import_sequences.read_sample_names(sample_list)
    with capture_stdout() as stdout:
        import_sequences.import_sequences(host, name, user, password,
                                          data_folder, sample_names,
                                          dryrun=True)
    output = stdout.text().strip()

    assert 'Data folder missing:\n  1784849c' in output
    assert 'Test ID missing:\n  11fad1b9' in output
    assert 'Already exists in database:\n  38003695' in output


def test_import_sequences_manual():
    (name, host, _, user, password) = database_config('test')
    delete_if_in_database(new_samples)

    data_folder = here/'fixtures'/'working'/'samples'
    sample_list = here/'fixtures'/'sampleset'/'samples.20220909_HJ5KWDRX2.tsv'
    sample_names = import_sequences.read_sample_names(sample_list)
    with capture_stdout() as stdout:
        import_sequences.import_sequences(host, name, user, password,
                                          data_folder, sample_names)
    output = stdout.text().strip()
    actual_lines = [line.strip() for line in output.splitlines()]

    expected_lines = [
        f'Data folder: {data_folder}',
        'SKIPPING: 11fad1b9_CP338_H3_type_opt',
        'No test_id found.',
        'SKIPPING: 1784849c_CP338_G2_type_opt',
        'No sample folder found.',
        'SKIPPING: 38003695_CP336_A1_type_opt',
        'Sequence already in database and no update requested.',
        'Imported: 38003696_CP336_A10_type_opt',
        'Imported: ETHID_CP336_G12_positiveControl1_MT007544_1',
        'Imported: ETHID_CP336_H12_negativeControl_H2O',
    ]
    i = 1
    for (actual, expected) in zip(actual_lines, expected_lines):
        if actual != expected:
            print(f'actual:   "{actual}"')
            print(f'expected: "{expected}"')
        assert actual == expected, f"line {i}"
        i += 1


def test_import_sequences_automated():
    (name, host, _, user, password) = database_config('test')
    delete_if_in_database(new_samples)

    data_folder = here/'fixtures'/'working'/'samples'
    with capture_stdout() as stdout:
        with mock.patch.object(builtins, 'input', lambda _: 'y'):
            import_sequences.import_sequences(host, name, user, password,
                                              data_folder)
    output = stdout.text().strip()
    actual_lines = [line.strip() for line in output.splitlines()]

    expected_lines = [
        f'Data folder: {data_folder}',
        'Found 5 available samples in data folder.',
        'Proposing to import 4 new sequences not yet in the database.',
        'SKIPPING: 11fad1b9_CP338_H3_type_opt',
        'No test_id found.',
        'Imported: 38003696_CP336_A10_type_opt',
        'Imported: ETHID_CP336_G12_positiveControl1_MT007544_1',
        'Imported: ETHID_CP336_H12_negativeControl_H2O',
    ]
    i = 1
    for (actual, expected) in zip(actual_lines, expected_lines):
        if actual != expected:
            print(f'actual:   "{actual}"')
            print(f'expected: "{expected}"')
        assert actual == expected, f"line {i}"
        i += 1


########################################
# Tear-down                            #
########################################

def teardown_module():
    global connection
    if connection:
        delete_if_in_database(new_samples)
        connection.close()
    connection = None


########################################
# Main                                 #
########################################

# Imitate what pyTest would do if this script is called directly.
if __name__ == "__main__":
    try:
        setup_module()
        test_read_sample_names()
        test_read_sequence()
        test_import_sequences_dryrun()
        test_import_sequences_manual()
        test_import_sequences_automated()
    finally:
        teardown_module()
