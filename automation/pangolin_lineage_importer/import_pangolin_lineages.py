import psycopg2
import os
import subprocess
import csv
import time
from datetime import datetime


def import_lineages(db_host, db_name, db_user, db_password):
    # Connect to database
    db_connection = "dbname=\'" + db_name + \
                    "\' user=\'" + db_user + \
                    "\' host=\'" + db_host + \
                    "\' password=\'" + db_password + "\'"
    try:
        conn = psycopg2.connect(db_connection)
    except Exception as e:
        raise Exception("I am unable to connect to the database.", e)

    # Create working directory
    subprocess.run(["mkdir", "-p", "/wkdir"])

    # Fetch sequences
    fetch_sql = """
        select
          cs.sample_name,
          cs.seq
        from
          consensus_sequence_nextclade_data nd
          join consensus_sequence cs on nd.sample_name = cs.sample_name
        where nd.pangolin_status is null
        limit 2000;
    """
    with conn.cursor() as cursor:
        cursor.execute(fetch_sql)
        sequences = cursor.fetchall()
    fasta = ""
    for sample_name, seq in sequences:
        fasta += ">{}\n{}\n\n".format(sample_name, seq)
    with open('/wkdir/sequences.fasta', 'w') as f:
        f.write(fasta)

    # Run pangolin
    subprocess.run(["conda", "run", "--no-capture-output", "-n", "pangolin", "pangolin", "-o", "/wkdir/", "/wkdir/sequences.fasta"],
                   stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL,
                   check=True)

    # Write pangolin results into the database
    number_inserted = 0
    insert_sql = """
        update consensus_sequence_nextclade_data
        set
          pangolin_lineage = %s,
          pangolin_learn_version = %s,
          pangolin_status = %s,
          pangolin_note = %s
        where sample_name = %s;
    """
    with open('/wkdir/lineage_report.csv', newline='') as f:
        with conn.cursor() as cursor:
            reader = csv.reader(f, delimiter=',')
            next(reader)  # Skip header
            for row in reader:
                cursor.execute(insert_sql, (row[1], row[9], row[11], row[12], row[0]))
                conn.commit()
                number_inserted += 1

    # Delete working directory
    subprocess.run(["rm", "-r", "/wkdir"])

    return number_inserted

while True:
    number_inserted = import_lineages(os.getenv("DB_HOST"), os.getenv("DB_DBNAME"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"))
    print("[{}] Imported the pangolin lineage for {} sequences".format(datetime.now(), number_inserted), flush=True)
    if number_inserted == 0:
        sleep_seconds = int(os.getenv("CHECK_FOR_NEW_DATA_INTERVAL_SECONDS"))
        print("[{}] Time to sleep. Back in {} seconds".format(datetime.now(), sleep_seconds), flush=True)
        time.sleep(sleep_seconds)
