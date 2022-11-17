#!/usr/bin/env python

import argparse
import io
from collections import ChainMap
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import psycopg2
import yaml
from Bio import SeqIO
from paramiko.client import SSHClient, WarningPolicy
from paramiko.ssh_exception import AuthenticationException
from psycopg2.extras import execute_batch

from extract_sample_conditions import ExtendedCondition, extract_sample_condition


def main():
    args = parse_args()
    conn = connect_db(args)
    try:
        new_sequences = fetch_sequences(conn, args)
        update_db(conn, new_sequences, args.reimport)
    finally:
        conn.close()


def parse_args():

    parser = argparse.ArgumentParser(
        description="Fetch latest consesus sequences from V-Pipe"
    )
    parser.add_argument(
        "--db-host", type=str, required=False, help="Host of sars_cov_2 database."
    )
    parser.add_argument(
        "--db-name", type=str, required=False, help="DB name of sars_cov_2 database."
    )
    parser.add_argument(
        "--db-username",
        type=str,
        required=False,
        help="Username for sars_cov_2 database.",
    )
    parser.add_argument(
        "--db-password",
        type=str,
        required=False,
        help="Password for sars_cov_2 database.",
    )

    parser.add_argument(
        "--db-port",
        type=int,
        default=5432,
        required=False,
        help="Port of sars_cov_2 database.",
    )
    parser.add_argument(
        "--cluster-name",
        type=str,
        required=False,
        help="computing cluster name where V-pipe is running",
    )
    parser.add_argument(
        "--cluster-user",
        type=str,
        required=False,
        help="user name to connect to computing cluster name where V-pipe is running",
    )

    parser.add_argument(
        "--cluster-keyfile",
        type=str,
        required=False,
        help=(
            "private key file to connect to computing cluster name where V-pipe is"
            " running"
        ),
    )

    parser.add_argument(
        "--cluster-uploads",
        type=str,
        required=False,
        help="location of V-pipe uploads folder on computing cluster",
    )

    parser.add_argument(
        "--config-filepath",
        type=str,
        default=None,
        help="Relative filepath to database connection config file.",
    )

    parser.add_argument(
        "--reimport",
        help="reimport sample/batches",
        action="store_const",
        const=True,
        default=False,
    )

    parser.add_argument(
        "what_to_import",
        nargs="*",
        help="limit imports for given SAMPLE or SAMPLE-BATCH arguments",
    )

    args = parser.parse_args()

    if args.config_filepath is not None:

        args_from_file = parse_yaml(args.config_filepath)
        for key, value in args_from_file.items():
            if getattr(args, key, None) is None:
                setattr(args, key, value)

    for action in parser._actions:
        name = action.dest
        if name == "help":
            continue
        if getattr(args, name, None) is None:
            raise ValueError(f"argument {action.option_strings[0]} missing")

    return args


def parse_yaml(path):
    with open(path, "r") as stream:
        config_yaml = yaml.safe_load(stream)
    server_config = config_yaml["default"]["database"]["server"]
    for k in ("host", "port", "username", "password"):
        server_config["db_" + k] = server_config.pop(k, None)

    server_config["db_name"] = server_config.pop("dbname", None)

    cluster_config = config_yaml["default"]["cluster"]
    for k in ("name", "user", "keyfile", "uploads"):
        cluster_config["cluster_" + k] = cluster_config.pop(k, None)
    return dict(**server_config, **cluster_config)


def connect_db(args):

    try:
        conn = psycopg2.connect(
            f"dbname='{args.db_name}' user='{args.db_username}' host='{args.db_host}'"
            f" password='{args.db_password}' port={args.db_port}"
        )

    except Exception as e:
        raise Exception("cannot connect to the database.", e)

    cursor = conn.cursor()
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS imported_samples (
        sample_name TEXT,
        batch_name TEXT,
        timestamp_processed TIMESTAMP,
        timestamp_imported TIMESTAMP NOT NULL DEFAULT NOW()
        );
    CREATE INDEX IF NOT EXISTS ix_imported_samples ON imported_samples (
            sample_name, batch_name
        );
    """
    )
    conn.commit()

    return conn


def fetch_sequences(conn_db, args):

    uploads_folder = args.cluster_uploads
    reimport = args.reimport

    restrict_to = [
        sample_batch_name.rsplit("-", 1) for sample_batch_name in args.what_to_import
    ]

    client_ssh = connect_ssh(args)

    stdin, stdout, stderr = client_ssh.exec_command(f"list {uploads_folder}")

    err = str(stderr.read(), "utf-8").split("\n")
    for line in err:
        if line:
            print(f"STDERR: {line}")

    out = str(stdout.read(), "utf-8").split("\n")

    print(len(out) - 2, "samples linked in uploads folder")

    identifiers = []

    for line in out[1:-1]:
        sample_batch_name, timestamp = line.split(" ", 1)
        ts = datetime.strptime(timestamp, "%Y %m %d %H %M %S")
        sample, batch = sample_batch_name.rsplit("-", 1)

        condition, extended_condition = extract_sample_condition(sample)
        if 0:
            if condition != ExtendedCondition.ETHZ_ID_SAMPLE:
                print("skip sample", sample, condition)
                continue

        # in case restrict_to is given only import samples with match sample name
        # (and batch name if provided)
        match = not restrict_to
        for rs, *rb in restrict_to:
            if rs == sample:
                if not rb or rb == batch:
                    match = True

        if not match:
            continue

        if reimport or not in_db(conn_db, sample, batch, ts):
            identifiers.append((sample, batch, ts))
        else:
            print(
                f"skip {sample}-{batch} which was already imported, specify"
                " --reimport to force this"
            )

    stdin.close()
    print("found", len(identifiers), "samples to import")

    def download(chunk):
        print("download sequences for", len(chunk), "samples")

        client_ssh = connect_ssh(args)
        all_sequences = dict()

        for sample, batch, ts in chunk:

            stdin, stdout, stderr = client_ssh.exec_command(
                f"import {uploads_folder} {sample}-{batch}/"
            )
            out = str(stdout.read(), "utf-8").split("\n")
            err = str(stderr.read(), "utf-8").split("\n")
            for line in err:
                if line:
                    print(f"STDERR: {line}")

            filename = None

            sequences = dict()
            sequence_lines = []

            for line in out:
                if filename is None:
                    filename = line.strip()
                    continue
                if line == "---":
                    sequences[filename] = "".join(sequence_lines)
                    sequence_lines.clear()
                    filename = None
                    continue
                sequence_lines.append(line)

            all_sequences[sample, batch, ts] = sequences

        return all_sequences

    # one thread per 20 samples, but not more than 20 threads and always at least
    # one thread:
    n = max(min(20, len(identifiers) // 20), 1)

    with ThreadPoolExecutor(n) as executor:
        chunks = [identifiers[i::n] for i in range(n)]
        sequences = list(executor.map(download, chunks))

    new_sequences = ChainMap(*sequences)
    num_bytes = sum(
        len(sequence)
        for sequences in new_sequences.values()
        for sequence in sequences.values()
    )

    mb = num_bytes / (1024 * 1024)

    if mb >= 1:
        print(f"{mb:.1f} MB transmitted")
    else:
        kb = num_bytes / 1024
        print(f"{kb:.1f} KB transmitted")

    return new_sequences


def update_db(conn, new_sequences, update):

    import_sequences(conn, new_sequences, update)
    update_imported_samples_table(conn, new_sequences)


def import_sequences(conn, new_sequences, update):
    ALIGNED = "ref_majority_dels.fasta"
    UNALIGNED = "consensus.bcftools.fasta"
    DEST_TABLE = "consensus_sequence"

    cursor = conn.cursor()

    for (sample, batch, ts), sequences in new_sequences.items():
        seq_aligned = sequences.get(ALIGNED)
        # todo: unaligned sequences need fix due to insertions
        seq_unaligned = sequences.get(UNALIGNED)

        if seq_aligned is None and seq_unaligned is None:
            print(
                f"STDERR: neither {ALIGNED} nor {UNALIGNED} where found for"
                f" {sample}-{batch}"
            )
            continue
        elif seq_aligned is None:
            print(f"STDERR: {ALIGNED} not found for {sample}-{batch}")
            continue
        elif seq_unaligned is None:
            print(f"STDERR: {UNALIGNED} not found for {sample}-{batch}")
            continue

        def parse_single(sequence):
            seqs = list(SeqIO.parse(io.StringIO(sequence), "fasta"))
            assert len(seqs) == 1
            return str(seqs[0].seq)

        seq_aligned = parse_single(seq_aligned)
        seq_unaligned = parse_single(seq_unaligned)

        ethid = sample.split("_")[0]
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

        try:
            cursor.execute(
                f"INSERT INTO {DEST_TABLE}"
                " (sample_name, seq_aligned, seq_unaligned, sequencing_batch, "
                "  sequencing_plate, sequencing_plate_well)"
                " VALUES(%s, %s, %s, %s, %s, %s)",
                (sample, seq_aligned, seq_unaligned, batch, plate, well),
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
                    (seq_aligned, seq_unaligned, batch, sample),
                )
                # Drop rows in nextclade tables so nextclade will be re-run on
                # the updated sequences
                for table in (
                    "consensus_sequence_mutation_nucleotide",
                    "consensus_sequence_mutation_aa",
                    "consensus_sequence_meta",
                ):
                    cursor.execute(
                        f"DELETE FROM {table} WHERE sample_name = %s",
                        (sample,),
                    )
            else:
                print(
                    "Not adding {}-{} because update = False and sample"
                    " already in table.".format(sample, batch)
                )
            conn.commit()


def update_imported_samples_table(conn, new_sequences):

    cursor = conn.cursor()
    execute_batch(
        cursor,
        """
        INSERT INTO imported_samples (sample_name, batch_name, timestamp_processed)
        VALUES (%s, %s, %s)
        """,
        new_sequences.keys(),
    )
    conn.commit()
    print("updated imported_samples table")


def connect_ssh(args):

    client = SSHClient()
    client.set_missing_host_key_policy(WarningPolicy())
    try:
        client.connect(
            args.cluster_name,
            username=args.cluster_user,
            key_filename=args.cluster_keyfile,
        )
        return client
    except AuthenticationException:
        raise OSError(
            f"Authentication to {args.cluster_user}@{args.cluster_name} using"
            f" keyfile {args.cluster_keyfile} failed."
        ) from None


def in_db(conn, sample, batch, timestamp):
    cursor = conn.cursor()
    cursor.execute(
        """SELECT COUNT(*) FROM imported_samples
               WHERE sample_name = %s AND batch_name = %s and timestamp_processed >= %s
               """,
        (sample, batch, timestamp),
    )
    matches = cursor.fetchall()[0][0]
    return matches > 0


if __name__ == "__main__":
    main()
