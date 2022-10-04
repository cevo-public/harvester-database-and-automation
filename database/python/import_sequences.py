"""
Imports V-pipe results into Vineyard database.

This script is usually run on Euler after a new batch of samples has
been processed. First and foremost, it imports the consensus sequences
for Covid samples that S3C was tasked to process. As opposed to other
samples that V-pipe may also process, like from wastewater monitoring,
the S3C samples should already have corresponding metadata in the
Vineyard database, from an earlier step in the data pipeline when that
metadata was received in the first place. The only S3C samples that
wouldn't have metadata are (positive or negative) controls, which are
often added to the sequencing plates for reasons of quality control.
"""

import psycopg2
from Bio import SeqIO
from pandas import read_csv
import argparse
import subprocess
import os
from pathlib import Path
from datetime import datetime


def read_sample_names(file):
    """
    Retrieves the sample names from the given file.

    The `file` is expected to be a tab-separated `.tsv` file without
    a header row, where the sample names are in the first column.
    """
    table = read_csv(file, header=None, sep='\t')
    sample_names = table.iloc[:, 0].tolist()
    return sample_names


def read_sequence(file):
    """Reads a `.fasta` file and returns the sequence as a string."""
    seqs = list(SeqIO.parse(file, "fasta"))
    assert len(seqs) == 1
    return str(seqs[0].seq)


def import_sequences(db_host, db_name, db_user, db_password,
                     data_folder, sample_names=None, batch=None,
                     update=False, dryrun=False):
    """
    Imports results for given `sample_names` as found in `data_folder`.

    Skips samples for which results (i.e. consensus sequences) are already
    in the database, unless `update` is `True`. A specific batch, such
    as `'20220909_HJ5KWDRX2'` can be selected from folders in `data_folder`,
    and must be when updating.
    """

    # Connect to database.
    try:
        conn = psycopg2.connect(dbname=db_name, host=db_host,
                                user=db_user, password=db_password,
                                connect_timeout=3)
    except Exception:
        raise ConnectionError("Unable to connect to the database.")

    # Accept string of Path object for data folder.
    data_folder = Path(data_folder)
    print(f'Data folder: {data_folder}')

    # Samples must be explicitly named when updating the database table.
    if update and not sample_names:
        raise ValueError("You need to specify a list of sample names "
                         "if you want to update the table. Tread carefully!")

    # If no samples named, import new samples found in data folder.
    if not sample_names:
        available = set(item.name for item in data_folder.iterdir()
                        if item.is_dir())
        print(f'Found {len(available)} available samples in data folder.')
        with conn.cursor() as cursor:
            cursor.execute("SELECT sample_name FROM consensus_sequence "
                           "WHERE seq_aligned IS NOT NULL")
            imported = set(name for (name,) in cursor.fetchall())
        sample_names = sorted(available - imported)
        print(f"Proposing to import {len(sample_names)} new sequences "
              "not yet in the database.")
        while True:
            answer = input("Proceed with the import? (y/n):\n")
            if answer == "y":
                break
            if answer == "n":
                exit(0)
            print("You must enter either 'y' or 'n'.")

    # Keep track of samples we'll skip for various reasons.
    folder_missing = []
    no_test_id     = []
    plate_missing  = []
    invalid_ethid  = []
    batch_missing  = []
    file_missing   = []
    no_seq_center  = []
    already_exists = []

    # Iterate through list of samples to import.
    cursor = conn.cursor()
    for sample_name in sample_names:

        # Check that sample folder exists.
        if not (data_folder/sample_name).is_dir():
            print(f"SKIPPING: {sample_name}")
            print("No sample folder found.")
            folder_missing.append(sample_name)
            continue

        # Sample number is what's before the first separator.
        sample_number, *rest = sample_name.split("_")

        # Literal "ETHID" marks a control, i.e. not an actual sample.
        # Actual samples must already have metadata in the database.
        test_id = None
        is_control = False
        if sample_number == 'ETHID':
            is_control = True
        else:
            cursor.execute("SELECT test_id FROM test_metadata "
                           f"WHERE test_id LIKE '%/{sample_number}'")
            values = [row[0] for row in cursor.fetchall()]
            if not values:
                print(f"SKIPPING: {sample_name}")
                print("No test_id found.")
                no_test_id.append(sample_name)
                continue
            if len(values) > 1:
                print(f"SKIPPING: {sample_name}")
                print(f"Multiple test_ids: {values}")
                no_test_id.append(sample_name)
                continue
            test_id = values[0]

        # Try to get sequencing plate and sequencing well from sample name.
        if len(rest) == 4 or (is_control and len(rest) == 5):
            (plate, well) = rest[:2]

        # Query plate and well from database if name could not be parsed.
        elif test_id:
            print(f"Sample name {sample_name} did not follow "
                  "ETHID_PLATE_WELL_TYPE_OPT convention.")
            print('Querying database for sequencing plate and well.')

            cursor.execute("SELECT sequencing_plate, sequencing_plate_well "
                           "FROM test_plate_mapping "
                           "WHERE test_id=%s", (test_id,))
            rows = cursor.fetchall()
            if rows:
                (plate, well) = rows[0]
            else:
                print(f"SKIPPING: {sample_name}")
                print(f"Did not find {test_id=} in table test_plate_mapping.")
                plate_missing.append(sample_name)
                continue

        # Otherwise it must be a control without plate/well information.
        else:
            print(f"SKIPPING: {sample_name}")
            print("Could not determine sequencing plate.")
            plate_missing.append(sample_name)
            continue

        # ETH-ID should be a number, except for controls.
        try:
            ethid = int(sample_number)
        except ValueError:
            if is_control:
                ethid = None
            else:
                print(f"SKIPPING: {sample_name}")
                print("Sample number is not an integer.")
                invalid_ethid.append(sample_name)
                continue

        # If no specific batch was chosen, take sequences from earliest batch.
        # There would usually be just one batch though. In the event of a
        # re-run, you want to specify the correct batch.
        if not batch:
            batches = sorted(item.name
                             for item in (data_folder/sample_name).iterdir()
                             if item.is_dir())
            batch_folder = data_folder/sample_name/batches[0]
        else:
            batch_folder = data_folder/sample_name/batch
        if not batch_folder.exists():
            print(f"SKIPPING: {sample_name}")
            print(f"Batch folder does not exist: {batch_folder}.")
            batch_missing.append(sample_name)
            continue

        # Read consensus sequence from files.
        folder = batch_folder/'references'
        file = folder/'ref_majority_dels.fasta'
        if not file.exists():
            print(f"SKIPPING: {sample_name}")
            print(f"File not found: {file}")
            file_missing.append(sample_name)
            continue
        seq_aligned = read_sequence(file)
        file = folder/'consensus_ambig.bcftools.fasta'
        if not file.exists():
            print(f"SKIPPING: {sample_name}")
            print(f"File not found: {file}")
            file_missing.append(sample_name)
            continue
        seq_unaligned = read_sequence(file)

        # Look up sequencing center.
        cursor.execute(
            "SELECT sequencing_center "
            "FROM sequencing_plate "
            "WHERE sequencing_plate_name=%s",
            (plate,),
        )
        rows = cursor.fetchall()
        if len(rows) > 1:
            print(f"SKIPPING: {sample_name}")
            print("Found multiple sequencing centers.")
            no_seq_center.append(sample_name)
            continue
        if rows:
            sequencing_center = rows[0][0]
        elif is_control:
            sequencing_center = 'gfb'
        else:
            cursor.execute("select extraction_plate from test_plate_mapping "
                          f"where test_id like '%/{sample_number}'")
            rows = cursor.fetchall()
            if len(rows) != 1:
                print(f"SKIPPING: {sample_name}")
                print("Failed to look up extraction plate.")
                no_seq_center.append(sample_name)
                continue
            extraction_plate = rows[0][0]
            cursor.execute("select sequencing_center from extraction_plate "
                           "where extraction_plate_name=%s",
                           (extraction_plate,))
            rows = cursor.fetchall()
            if len(rows) != 1:
                print(f"SKIPPING: {sample_name}")
                print("Failed to look up sequencing center.")
                no_seq_center.append(sample_name)
                continue
            sequencing_center = rows[0][0]

        # Add results for this sample to database.
        try:
            cursor.execute(
                "INSERT INTO consensus_sequence ("
                "sample_name, "
                "seq_aligned, "
                "seq_unaligned, "
                "sequencing_batch, "
                "sequencing_plate, "
                "sequencing_plate_well, "
                "sequencing_center, "
                "ethid, "
                "insert_date) "
                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    sample_name,
                    seq_aligned,
                    seq_unaligned,
                    batch_folder.name,
                    plate,
                    well,
                    sequencing_center,
                    ethid,
                    datetime.now(),
                ),
            )

            # Add entry to separate metadata table as well.
            cursor.execute(
                "INSERT INTO consensus_sequence_meta (sample_name) VALUES(%s)",
                (sample_name,),
            )

            # Insert new data into database.
            if not dryrun:
                conn.commit()
            print(f"Imported: {sample_name}")

        # Revert if insertion failed. Possibly update database instead.
        except psycopg2.errors.UniqueViolation:

            conn.rollback()

            if not update:
                print(f"SKIPPING: {sample_name}")
                print("Sequence already in database and no update requested.")
                already_exists.append(sample_name)
                continue

            cursor.execute(
                "UPDATE consensus_sequence "
                "SET "
                "seq_aligned = %s, "
                "seq_unaligned = %s, "
                "sequencing_batch = %s, "
                "update_date = %s "
                "WHERE sample_name = %s",
                (
                    seq_aligned,
                    seq_unaligned,
                    batch_folder.name,
                    datetime.now(),
                    sample_name,
                ),
            )

            # Re-insert sample in sequence metadata tables so that other
            # tools (such as Nextclade) run on it once again.
            tables = (
                "consensus_sequence_mutation_nucleotide",
                "consensus_sequence_mutation_aa",
                "consensus_sequence_meta",
            )
            for table in tables:
                cursor.execute(f"DELETE FROM {table} WHERE sample_name = %s",
                               (sample_name,))
            cursor.execute(
                "INSERT INTO consensus_sequence_meta "
                "(sample_name) VALUES(%s)",
                (sample_name,))

            if not dryrun:
                conn.commit()
            print(f"Updated: {sample_name}")

    cursor.close()
    conn.close()

    # Display collected skip reasons if this was a dry run.
    if dryrun:
        print()
        print('Dry-run summary')
        print('---------------')
        if folder_missing:
            print('Data folder missing:')
            for sample_name in folder_missing:
                print(f'  {sample_name}')
        if no_test_id:
            print('Test ID missing:')
            for sample_name in no_test_id:
                print(f'  {sample_name}')
        if plate_missing:
            print('Sequencing plate missing:')
            for sample_name in plate_missing:
                print(f'  {sample_name}')
        if invalid_ethid:
            print('ETH-ID invalid:')
            for sample_name in invalid_ethid:
                print(f'  {sample_name}')
        if batch_missing:
            print('Batch folder missing:')
            for sample_name in batch_missing:
                print(f'  {sample_name}')
        if file_missing:
            print('Sequence file missing:')
            for sample_name in file_missing:
                print(f'  {sample_name}')
        if no_seq_center:
            print('Sequencing center unknown:')
            for sample_name in no_seq_center:
                print(f'  {sample_name}')
        if already_exists:
            print('Already exists in database:')
            for sample_name in already_exists:
                print(f'  {sample_name}')


def run_automated():
    print("Running in automated mode.")

    db_host     = os.getenv("DB_HOST"),
    db_name     = os.getenv("DB_DBNAME"),
    db_user     = os.getenv("DB_USER"),
    db_password = os.getenv("DB_PASSWORD"),
    data_folder = "/mnt/pangolin/consensus_data/batch/samples"

    import_sequences(db_host, db_name, db_user, db_password, data_folder)


def run_euler():
    print("Running in Euler mode.")

    db_name = os.environ.get("DB_NAME")
    db_host = os.environ.get("DB_HOST")
    db_user = os.environ.get("DB_USER")

    if db_name is None:
        db_name = input("Enter database name:\n")
    if db_host is None:
        db_host = input("Enter database host:\n")

    batch = input("Enter batch name to import:\n")

    update = None
    while update is None:
        answer = input("Do you want to overwrite existing sequences "
                       "if already in database? (y/n):\n")
        if answer == "y":
            update = True
        elif answer == "n":
            update = False
        else:
            print("You must enter either 'y' or 'n'.")

    if db_user is None:
        db_user = input(f"Enter username for database {db_name}:\n")
    db_password = input(f"Enter password for user {db_user}:\n")

    project_folder = Path('/cluster/project/pangolin')
    data_folder    = project_folder/'working'/'samples'
    sample_list    = project_folder/'sampleset'/f'samples.{batch}.tsv'
    sample_names   = read_sample_names(sample_list)

    print("Importing consensus sequences.")
    import_sequences(db_host, db_name, db_user, db_password,
                     data_folder, sample_names, batch, update)

    # Import frameshift deletion diagnostics.
    print("Calling Rscript to import frameshift diagnostics.")
    subprocess.run([
        "Rscript",
        "--vanilla",
        "database/R/import_frameshift_deletion_diagnostic.R",
        "--samplesdir", data_folder,
        "--dbhost", db_host,
        "--dbuser", db_user,
        "--dbpassword", db_password,
        "--dbname", db_name,
        "--batch", batch,
        "--dbport", "5432",
    ], check=True)


def run_manual(dryrun=False):
    print("Running in manual mode.")

    db_name     = input("Enter database name:\n")
    db_host     = input("Enter database host:\n")
    db_user     = input(f'Enter username for database "{db_name}":\n')
    db_password = input(f'Enter password for user "{db_user}":\n')
    data_folder = input("Enter samples directory (no quotes!):\n")
    sample_list = input("Enter full path (no quotes!) to tab-separated "
                        "samples file with sample names in first column:\n")

    sample_names = read_sample_names(sample_list)

    import_sequences(db_host, db_name, db_user, db_password,
                     data_folder, sample_names,
                     dryrun=dryrun)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Import sequences into the database.",
    )
    parser.add_argument(
        "--automated",
        const=True,
        default=False,
        action="store_const",
        help="Run the script as part of the automation.",
    )
    parser.add_argument(
        "--euler",
        const=True,
        default=False,
        action="store_const",
        help="Run the script to fetch sequences from Euler.",
    )
    parser.add_argument(
        "--dryrun",
        const=True,
        default=False,
        action="store_const",
        help="Only perform a dry-run.",
    )
    args = parser.parse_args()

    if args.automated:
        run_automated()
    elif args.euler:
        run_euler()
    else:
        run_manual(args.dryrun)
