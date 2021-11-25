import subprocess
import psycopg2
from Bio import SeqIO
import os
import argparse
import pandas as pd


def import_sequences(data_dir, db_host, db_name, db_user, db_password, sample_names=None, update=False, batch=None):
    DEST_TABLE = "consensus_sequence"

    # Connect to database
    db_connection = "dbname=\'" + db_name + \
                    "\' user=\'" + db_user + \
                    "\' host=\'" + db_host + \
                    "\' password=\'" + db_password + "\'"
    try:
        conn = psycopg2.connect(db_connection)
    except Exception as e:
        raise Exception("I am unable to connect to the database.", e)

    if sample_names is None:
        # Fetch already included sample names
        with conn.cursor() as cursor:
            cursor.execute("SELECT sample_name FROM " + DEST_TABLE + " WHERE seq IS NOT NULL")
            imported_sample_names_tuples = cursor.fetchall()
        imported_sample_names = [tuple[0] for tuple in imported_sample_names_tuples]

        # Get set of new sample names with sequences available
        available_sample_names = os.listdir(path=data_dir)
        found_sample_names = list(set(available_sample_names) - set(imported_sample_names))
        print("Proposing to import {} new sequences not yet in the database".format(len(found_sample_names)))
        IMPORT = None
        while IMPORT is None:
            IMPORT_INPUT = input("Do you want to import all these sequences? (y/n):\n")
            if IMPORT_INPUT == "y":
                IMPORT = True
            elif IMPORT_INPUT == "n":
                IMPORT = False
                exit(0)
            else:
                print("You must enter either 'y' or 'n'.")
    else:
        # See how many of the specified samples are available
        available_sample_names = os.listdir(path=data_dir)
        found_sample_names = list(set(available_sample_names).intersection(set(sample_names)))
        print("Going to import {} out of {} specified samples that were found in {}.".format(
            len(found_sample_names),
            len(sample_names),
            data_dir
        ))
        print("These samples not found: {}".format(
            set(sample_names) - set(found_sample_names)
        ))

    # Require that specific sample names be provided in order to update the table
    if sample_names is None and update:
        raise ValueError('You need to specify a list of sample names if you want to update the table. Tread carefully!')

    # Iterate through the sequences, importing them into the database
    cursor = conn.cursor()
    i = 0
    missing_seq_file = []
    for sample_name in found_sample_names:
        i += 1
        if batch is None:
            batch_to_import = os.listdir(path=data_dir + "/" + sample_name)[0]  # take sequences from 1st available batch -- you may want to specify a different batch folder in the event of a re-run
        else:
            batch_to_import = batch
        seq_file = data_dir + "/" + sample_name + "/" + batch_to_import + "/references/ref_majority_dels.fasta"
        seqs = SeqIO.parse(seq_file, "fasta")
        try:
            for record in seqs:

                header = record.id
                seq = str(record.seq)

                try:
                    cursor.execute(
                        "INSERT INTO " + DEST_TABLE + " (sample_name, header, seq, sequencing_batch) VALUES(%s, %s, %s, %s)",
                        (sample_name, header, seq, batch_to_import)
                    )
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()  # revert the failed cursor.execute(INSERT) and instead try an UPDATE
                    if update:
                        cursor.execute(
                            "UPDATE " + DEST_TABLE + " SET seq = %s, header = %s, sequencing_batch = %s WHERE sample_name = %s",
                            (seq, header, batch_to_import, sample_name)
                        )
                        # Drop rows in nextclade tables so nextclade will be re-run on the updated sequences
                        cursor.execute(
                            "DELETE FROM consensus_sequence_nextclade_mutation_aa WHERE sample_name = %s",
                            (sample_name,)
                        )
                        cursor.execute(
                            "DELETE FROM consensus_sequence_mutation_nucleotide WHERE sample_name = %s",
                            (sample_name,)
                        )
                        cursor.execute(
                            "DELETE FROM consensus_sequence_nextclade_data WHERE sample_name = %s",
                            (sample_name,)
                        )
                        # Drop values in columns generated by import_sequence_diagnostic.R in consensus_sequence
                        # so diagnostic will be re-run on the updated sequences
                        cursor.execute(
                            "UPDATE " + DEST_TABLE + " SET divergence = NULL, excess_divergence = NULL, number_n = NULL, " \
                                                     "number_gaps = NULL,  clusters = NULL, gaps = NULL, all_snps = NULL, " \
                                                     "flagging_reason = NULL WHERE sample_name = %s",
                            (sample_name,)
                        )
                    else:
                        print("Not adding {} because update = False and sample already in table.".format(sample_name))
            conn.commit()
            print("Importing sequence {}/{}".format(i, len(found_sample_names)))
        except FileNotFoundError as e:
            print("File not found, will not import sequence:" + seq_file)
            missing_seq_file.append(sample_name)
            pass

    cursor.close()
    conn.close()
    if len(missing_seq_file) > 0:
        raise Warning("These {} samples don't have a sequence file and were not imported:\n {}".format(
            len(missing_seq_file), '\n'.join(missing_seq_file)
    ))


parser = argparse.ArgumentParser(description='Import sequences into the database')
parser.add_argument('--automated', const=True, default=False, action="store_const",
                    help="Run the script as part of the automation")
parser.add_argument('--euler', const=True, default=False, action="store_const",
                    help="Run the script to fetch sequences from Euler")
args = parser.parse_args()

if args.automated:
    print("Running in automated mode.")
    import_sequences("/mnt/pangolin/consensus_data/batch/samples",
                     os.getenv("DB_HOST"),
                     os.getenv("DB_DBNAME"),
                     os.getenv("DB_USER"),
                     os.getenv("DB_PASSWORD"))


elif args.euler:
    print("Running in euler mode.")
    DB_NAME = input("Enter database name:\n")
    DB_HOST = input("Enter database host:\n")
    BATCH = input("Enter batch name to import:\n")

    UPDATE = None
    while UPDATE is None:
        UPDATE_INPUT = input("Do you want to overwrite existing sequences if already in database? (y/n):\n")
        if UPDATE_INPUT == "y":
            UPDATE = True
        elif UPDATE_INPUT == "n":
            UPDATE = False
        else:
            print("You must enter either 'y' or 'n'.")

    DB_USER = input("Enter username for database " + DB_NAME + ":\n")
    DB_PASSWORD = input("Enter password for user " + DB_USER + ":\n")
    SAMPLESET_TOPLEVEL_DIR = input("Enter samples directory (no quotes!):\n")
    SAMPLE_LIST = input("Enter full path (no quotes!) to the sample list directory:\n") + "/samples." + BATCH + ".tsv"

    # Get list of samples to import
    df = pd.read_csv(SAMPLE_LIST, names=['sample_name', 'sequencing_batch', 'heh?'], sep="\t")
    sample_list = df['sample_name'].tolist()

    # Import consensus sequences
    print("Importing consensus sequences.")
    import_sequences(SAMPLESET_TOPLEVEL_DIR, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD,
                     sample_names=sample_list, update=UPDATE, batch=BATCH)

    # Import frameshift deletion diagnostics
    print("Calling Rscript to import frameshift diagnostics.")
    process = subprocess.Popen(
        ["Rscript", "--vanilla",
         "database/R/import_frameshift_deletion_diagnostic.R",
         "--samplesdir", SAMPLESET_TOPLEVEL_DIR,
         "--dbhost", DB_HOST,
         "--dbuser", DB_USER,
         "--dbpassword", DB_PASSWORD,
         "--dbname", DB_NAME,
         "--batch", BATCH],
        stdout=subprocess.PIPE,
        text=True)

    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
        rc = process.poll()

else:
    print("Running in manual mode.")
    DB_NAME = input("Enter database name:\n")
    DB_HOST = input("Enter database host:\n")
    DB_USER = input("Enter username for database " + DB_NAME + ":\n")
    DB_PASSWORD = input("Enter password for user " + DB_USER + ":\n")
    SAMPLESET_TOPLEVEL_DIR = input("Enter samples directory (no quotes!):\n")
    SAMPLE_LIST = input("Enter full path (no quotes!) to tab-separated samples file with 1st column of sample names:\n")

    df = pd.read_csv(SAMPLE_LIST, names=['sample_name', 'sequencing_batch', 'heh?'], sep="\t", comment='#')  # 2nd 2 columns will be ignored, they're optional

    sample_list = df['sample_name'].tolist()
    import_sequences(SAMPLESET_TOPLEVEL_DIR, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, sample_names=sample_list)
