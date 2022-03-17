import argparse
import os
import subprocess

import pandas as pd
import psycopg2
from Bio import SeqIO


def import_sequences(
    data_dir,
    db_host,
    db_name,
    db_user,
    db_password,
    sample_names=None,
    update=False,
    batch=None,
):
    DEST_TABLE = "consensus_sequence"

    # Connect to database
    db_connection = (
        f"dbname='{db_name}' user='{db_user}' host='{db_host}'"
        f" password='{db_password}'"
    )
    try:
        conn = psycopg2.connect(db_connection)
    except Exception as e:
        raise Exception("I am unable to connect to the database.", e)

    if sample_names is None:
        # Fetch already included sample names
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT sample_name FROM {DEST_TABLE} WHERE seq_aligned IS NOT NULL"
            )
            imported_sample_names_tuples = cursor.fetchall()
        imported_sample_names = [name for (name,) in imported_sample_names_tuples]

        # Get set of new sample names with sequences available
        available_sample_names = os.listdir(path=data_dir)
        found_sample_names = list(
            set(available_sample_names) - set(imported_sample_names)
        )
        print(
            "Proposing to import {} new sequences not yet in the database".format(
                len(found_sample_names)
            )
        )
        while True:
            IMPORT_INPUT = input("Do you want to import all these sequences? (y/n):\n")
            if IMPORT_INPUT == "y":
                break
            elif IMPORT_INPUT == "n":
                exit(0)
            else:
                print("You must enter either 'y' or 'n'.")
    else:
        # See how many of the specified samples are available
        available_sample_names = os.listdir(path=data_dir)
        found_sample_names = sorted(set(available_sample_names) & set(sample_names))
        print(
            "Going to import {} out of {} specified samples that were"
            " found in {}.".format(len(found_sample_names), len(sample_names), data_dir)
        )
        print(
            "These samples not found: {}".format(
                sorted(set(sample_names) - set(found_sample_names))
            )
        )

    # Require that specific sample names be provided in order to update the table
    if sample_names is None and update:
        raise ValueError(
            "You need to specify a list of sample names if you want to update the"
            " table. Tread carefully!"
        )

    # Iterate through the sequences, importing them into the database
    cursor = conn.cursor()
    i = 0
    missing_seq_file = []

    def read_single(sample_name, file_name):
        path = os.path.join(
            data_dir, sample_name, batch_to_import, "references", file_name
        )
        seqs = list(SeqIO.parse(path, "fasta"))
        assert len(seqs) == 1
        return str(seqs[0].seq)

    for sample_name in found_sample_names:

        ethid = sample_name.split("_")[0]
        try:
            ethid = int(ethid)
            cursor.execute(
                """
                SELECT sequencing_plate, sequencing_plate_well
                FROM test_plate_mapping
                JOIN test_metadata
                ON test_plate_mapping.test_id = test_metadata.test_id
                WHERE ethid=%s;""",
                (ethid,),
            )
            rows = cursor.fetchall()
            if not rows:
                plate, well = None, None
            else:
                plate, well = rows[0]
        except ValueError:
            plate, well = None, None

        i += 1
        if batch is None:
            # take sequences from 1st available batch -- you may want to specify a
            # different batch folder in the event of a re-run
            batch_to_import = os.listdir(path=data_dir + "/" + sample_name)[0]
        else:
            batch_to_import = batch

        seq_aligned = read_single(sample_name, "ref_majority_dels.fasta")
        seq_unaligned = read_single(sample_name, "consensus_ambig.bcftools.fasta")

        try:
            cursor.execute(
                f"INSERT INTO {DEST_TABLE}"
                " (sample_name, seq_aligned, seq_unaligned, sequencing_batch, "
                "  sequencing_plate, sequencing_plate_well)"
                " VALUES(%s, %s, %s, %s, %s, %s)",
                (sample_name, seq_aligned, seq_unaligned, batch_to_import, plate, well),
            )
            conn.commit()
        except psycopg2.errors.UniqueViolation:
            # revert the failed cursor.execute(INSERT) and instead try an UPDATE
            conn.rollback()
            if update:
                cursor.execute(
                    f"UPDATE {DEST_TABLE}"
                    " SET seq_aligned = %s, seq_unaligned = %s, sequencing_batch = %s"
                    " WHERE sample_name = %s",
                    (seq_aligned, seq_unaligned, batch_to_import, sample_name),
                )
                # Drop rows in nextclade tables so nextclade will be re-run on
                # the updated sequences
                for table in (
                    "consensus_sequence_mutation_nucleotide",
                    "consensus_sequence_mutation_aa",
                    "consensus_sequence_sequence_meta",
                ):
                    cursor.execute(
                        f"DELETE FROM {table} WHERE sample_name = %s",
                        (sample_name,),
                    )
            else:
                print(
                    "Not adding {} because update = False and sample"
                    " already in table.".format(sample_name)
                )
            conn.commit()
            print("Importing sequence {}/{}".format(i, len(found_sample_names)))
        except FileNotFoundError:
            print("File not found, will not import sequence:" + seq_file)
            missing_seq_file.append(sample_name)
            pass

    cursor.close()
    conn.close()
    if len(missing_seq_file) > 0:
        raise Warning(
            "These {} samples don't have a sequence file and were not"
            " imported:\n {}".format(len(missing_seq_file), "\n".join(missing_seq_file))
        )


def run_automated():
    print("Running in automated mode.")
    import_sequences(
        # "samples",
        "/mnt/pangolin/consensus_data/batch/samples",
        os.getenv("DB_HOST"),
        os.getenv("DB_DBNAME"),
        os.getenv("DB_USER"),
        os.getenv("DB_PASSWORD"),
        # os.listdir("samples"),
    )


def run_euler():
    print("Running in euler mode.")
    DB_NAME = input("Enter database name:\n")
    DB_HOST = input("Enter database host:\n")
    BATCH = input("Enter batch name to import:\n")

    UPDATE = None
    while UPDATE is None:
        UPDATE_INPUT = input(
            "Do you want to overwrite existing sequences if already in"
            " database? (y/n):\n"
        )
        if UPDATE_INPUT == "y":
            UPDATE = True
        elif UPDATE_INPUT == "n":
            UPDATE = False
        else:
            print("You must enter either 'y' or 'n'.")

    DB_USER = input(f"Enter username for database {DB_NAME}:\n")
    DB_PASSWORD = input(f"Enter password for user {DB_USER}:\n")
    SAMPLESET_TOPLEVEL_DIR = input("Enter samples directory (no quotes!):\n")
    SAMPLE_LIST_DIRECTORY = input(
        "Enter full path (no quotes!) to the sample" " list directory:\n"
    )
    SAMPLE_LIST = os.path.join(SAMPLE_LIST_DIRECTORY, f"samples.{BATCH}.tsv")

    # Get list of samples to import
    df = pd.read_csv(
        SAMPLE_LIST, names=["sample_name", "sequencing_batch", "heh?"], sep="\t"
    )
    sample_list = df["sample_name"].tolist()

    # Import consensus sequences
    print("Importing consensus sequences.")
    import_sequences(
        SAMPLESET_TOPLEVEL_DIR,
        DB_HOST,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
        sample_names=sample_list,
        batch=BATCH,
    )

    # Import frameshift deletion diagnostics
    print("Calling Rscript to import frameshift diagnostics.")
    process = subprocess.Popen(
        [
            "Rscript",
            "--vanilla",
            "database/R/import_frameshift_deletion_diagnostic.R",
            "--samplesdir",
            SAMPLESET_TOPLEVEL_DIR,
            "--dbhost",
            DB_HOST,
            "--dbuser",
            DB_USER,
            "--dbpassword",
            DB_PASSWORD,
            "--dbname",
            DB_NAME,
            "--batch",
            BATCH,
        ],
        stdout=subprocess.PIPE,
        text=True,
    )

    while True:
        output = process.stdout.readline()
        if output == "" and process.poll() is not None:
            break
        if output:
            print(output.rstrip())
        process.poll()


def run_manual():
    print("Running in manual mode.")
    DB_NAME = input("Enter database name:\n")
    DB_HOST = input("Enter database host:\n")
    DB_USER = input("Enter username for database {DB_NAME}:\n")
    DB_PASSWORD = input("Enter password for user {DB_USER}:\n")
    SAMPLESET_TOPLEVEL_DIR = input("Enter samples directory (no quotes!):\n")
    SAMPLE_LIST = input(
        "Enter full path (no quotes!) to tab-separated samples file with"
        " 1st column of sample names:\n"
    )

    df = pd.read_csv(
        SAMPLE_LIST,
        names=["sample_name", "sequencing_batch", "heh?"],
        sep="\t",
        comment="#",
    )  # 2nd 2 columns will be ignored, they're optional

    sample_list = df["sample_name"].tolist()
    import_sequences(
        SAMPLESET_TOPLEVEL_DIR,
        DB_HOST,
        DB_NAME,
        DB_USER,
        DB_PASSWORD,
        sample_names=sample_list,
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Import sequences into the database")
    parser.add_argument(
        "--automated",
        const=True,
        default=False,
        action="store_const",
        help="Run the script as part of the automation",
    )
    parser.add_argument(
        "--euler",
        const=True,
        default=False,
        action="store_const",
        help="Run the script to fetch sequences from Euler",
    )
    args = parser.parse_args()

    if args.automated:
        run_automated()

    elif args.euler:
        run_euler()

    else:
        run_manual()
