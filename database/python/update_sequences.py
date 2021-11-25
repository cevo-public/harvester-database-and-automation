import psycopg2
from Bio import SeqIO
import os
import argparse


def import_sequences(data_dir, db_host, db_name, db_user, db_password):
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

    # Get set of sample names with sequences available
    available_sample_names = os.listdir(path=data_dir)
    print("Going to update {} sequences".format(len(available_sample_names)))

    # Iterate through available sequences, importing them into the database
    cursor = conn.cursor()

    i = 0
    for sample_name in available_sample_names:
        i += 1
        batch_subdir = os.listdir(path=data_dir + "/" + sample_name)[0]
        seq_file = data_dir + "/" + sample_name + "/" + batch_subdir + "/references/ref_majority_dels.fasta"
        seqs = SeqIO.parse(seq_file, "fasta")
        for record in seqs:
            seq = str(record.seq)

            sql = "UPDATE " + DEST_TABLE + " SET seq = %s WHERE sample_name = %s"
            val = (seq, sample_name)
            cursor.execute(sql, val)

        if i % 100 == 0:
            print("Importing sequence {}".format(i))
            conn.commit()  # commit to database every 100 sequences
    conn.commit()
    cursor.close()
    conn.close()


parser = argparse.ArgumentParser(description='Import sequences into the database')
parser.add_argument('--automated', const=True, default=False,action="store_const",
                    help="Run the script as part of the automation")
args = parser.parse_args()

if args.automated:
    import_sequences("/mnt/pangolin/consensus_data/batch/samples",
                     os.getenv("DB_HOST"), os.getenv("DB_DBNAME"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"))
else:
    DB_NAME = input("Enter database name:\n")
    DB_HOST = input("Enter database host:\n")
    DB_USER = input("Enter username for database" + DB_NAME + ":\n")
    DB_PASSWORD = input("Enter password for user " + DB_USER + ":\n")
    SAMPLESET_TOPLEVEL_DIR = input("Enter samples directory (no quotes!):\n")

    import_sequences(SAMPLESET_TOPLEVEL_DIR, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)
