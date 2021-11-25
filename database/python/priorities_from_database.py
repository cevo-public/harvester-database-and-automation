# Modified from https://github.com/nextstrain/ncov/blob/master/scripts/priorities.py
# Larger (less negative) scores are the most genetically similar to the focal sequences
# Length of focal strains must be >1

import argparse
from random import shuffle
from collections import defaultdict
import numpy as np
from Bio import SeqIO
from scipy import sparse
import pandas as pd
import psycopg2
import yaml

def compactify_sequences(sparse_matrix, sequence_names):
    sequence_groups = defaultdict(list)
    for s, snps in zip(sequence_names, sparse_matrix):
        ind = snps.nonzero()
        vals = np.array(snps[ind])
        if len(ind[1]):
            sequence_groups[tuple(zip(ind[1], vals[0]))].append(s)
        else:
            sequence_groups[tuple()].append(s)

    return sequence_groups

INITIALISATION_LENGTH = 1000000

def sequence_to_int_array(s, fill_value=110, fill_gaps=True):
    seq = np.frombuffer(str(s).lower().encode('utf-8'), dtype=np.int8).copy()
    if fill_gaps:
        seq[(seq!=97) & (seq!=99) & (seq!=103) & (seq!=116)] = fill_value
    else:
        seq[(seq!=97) & (seq!=99) & (seq!=103) & (seq!=116) & (seq!=45)] = fill_value
    return seq

# Function adapted from https://github.com/gtonkinhill/pairsnp-python
def calculate_snp_matrix(seq_tuples, consensus, fill_value=110):
    # This function generate a sparse matrix where differences to the consensus are coded as integers.

    row = np.empty(INITIALISATION_LENGTH)
    col = np.empty(INITIALISATION_LENGTH, dtype=np.int64)
    val = np.empty(INITIALISATION_LENGTH, dtype=np.int8)

    r = 0
    n_snps = 0
    nseqs = 0
    seq_names = []
    filled_positions = []
    current_length = INITIALISATION_LENGTH

    for h,s in seq_tuples:
        align_length = len(consensus)

        nseqs +=1
        seq_names.append(h)

        if(len(s)!=align_length):
            raise ValueError('Sequence ' + h + 'does\'t have the same length as the reference.')

        s = sequence_to_int_array(s, fill_value=fill_value)
        snps = (consensus!=s) & (s!=fill_value)
        right = n_snps + np.sum(snps)
        filled_positions.append(np.where(s==fill_value)[0])

        if right >= (current_length/2):
            current_length = current_length + INITIALISATION_LENGTH
            row.resize(current_length)
            col.resize(current_length)
            val.resize(current_length)

        row[n_snps:right] = r
        col[n_snps:right] = np.flatnonzero(snps)
        val[n_snps:right] = s[snps]
        r += 1
        n_snps = right

    if nseqs==0:
        raise ValueError('No sequences found!')

    row = row[0:right]
    col = col[0:right]
    val = val[0:right]

    sparse_snps = sparse.csc_matrix((val, (row, col)), shape=(nseqs, align_length))

    return {'snps': sparse_snps, 'consensus': consensus, 'names': seq_names, 'filled_positions': filled_positions}

# Function adapted from https://github.com/gtonkinhill/pairsnp-python
def calculate_distance_matrix(sparse_matrix_A, sparse_matrix_B, consensus):

    n_seqs_A = sparse_matrix_A.shape[0]
    n_seqs_B = sparse_matrix_B.shape[0]

    d = (1*(sparse_matrix_A==97)) * (sparse_matrix_B.transpose()==97)
    d = d + (1*(sparse_matrix_A==99) * (sparse_matrix_B.transpose()==99))
    d = d + (1*(sparse_matrix_A==103) * (sparse_matrix_B.transpose()==103))
    d = d + (1*(sparse_matrix_A==116) * (sparse_matrix_B.transpose()==116))

    d = d.todense()

    n_comp = (1*(sparse_matrix_A==110) * ((sparse_matrix_B==110).transpose())).todense()
    d = d + n_comp

    temp_total = np.zeros((n_seqs_A, n_seqs_B))
    temp_total[:] = (1*(sparse_matrix_A>0)).sum(1)
    temp_total += (1*(sparse_matrix_B>0)).sum(1).transpose()

    total_differences_shared = (1*(sparse_matrix_A>0)) * (sparse_matrix_B.transpose()>0)

    n_total = np.zeros((n_seqs_A, n_seqs_B))
    n_sum = (1*(sparse_matrix_A==110)).sum(1)
    n_total[:] = n_sum
    n_total += (1*(sparse_matrix_B==110)).sum(1).transpose()

    diff_n = n_total - 2*n_comp
    d = temp_total - total_differences_shared.todense() - d - diff_n

    return d

def get_alignment(conn, strains, table, sample_name_col, seq_col):
    with conn.cursor() as cursor:
        sql = "SELECT " + sample_name_col + ", " + seq_col + " FROM " + table + " WHERE " + sample_name_col + " IN %s"
        # print(cursor.mogrify(sql, (strains,)))
        cursor.execute(sql, (strains,))
        strains_tuples = cursor.fetchall()
    strains_found = [tuple[0] for tuple in strains_tuples]
    strains_missing = list(set(strains) - set(strains_found))

    if len(strains_missing) > 0:
        raise ValueError('These strains not found in table gisaid_api_sequence: {}'.format(strains_missing[0:10]))
    return(strains_tuples)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Assign priority to context strains based on genetic similarity to focal strains.'
    )
    parser.add_argument('--focal-strains', type=str, required=True,
                        help="Tab-delimited file where focal strain names are in column 'strain'.")
    parser.add_argument('--context-strains', type=str, required=True,
                        help="Tab-delimited file where context strain names are in column 'strain'.")
    parser.add_argument('--reference', type=str, required=True, help="Fasta file with reference sequence.")
    parser.add_argument('--outfile', type=str, required=True, help="File to output priority results to.")
    parser.add_argument('--dbhost', type=str, required=False, help="Host of sars_cov_2 database.")
    parser.add_argument('--dbname', type=str, required=False, help="DB name of sars_cov_2 database.")
    parser.add_argument('--username', type=str, required=False, help="Username for sars_cov_2 database.")
    parser.add_argument('--password', type=str, required=False, help="Password for sars_cov_2 database.")
    parser.add_argument('--focal-strain-table', type=str, required=False, default='gisaid_api_sequence',
                        help="Name of table to take focal sequence data from.")
    parser.add_argument('--context-strain-table', type=str, required=False, default='gisaid_api_sequence',
                        help="Name of table to take context sequence data from.")
    parser.add_argument('--focal-sample-name-col', type=str, required=False, default='strain',
                        help="Column name matching 'strain' entries in focal-strains input.")
    parser.add_argument('--context-sample-name-col', type=str, required=False, default='strain',
                        help="Column name matching 'strain' entries in context-strains input.")
    parser.add_argument('--focal-seq-col', type=str, required=False, default='seq_aligned',
                        help="Column name containing focal sequences in the database table.")
    parser.add_argument('--context-seq-col', type=str, required=False, default='seq_aligned',
                        help="Column name containing context sequences in the database table.")
    parser.add_argument('--automated', const=True, default=False, action="store_const",
                        help="Run the script as part of the automation.")
    parser.add_argument('--config-filepath', type=str, default="database/config.yml",
                        help="Relative filepath to database connection config file.")
    args = parser.parse_args()

    FOCAL_LIST = args.focal_strains
    CONTEXT_LIST = args.context_strains
    REFERENCE = args.reference
    OUTFILE = args.outfile
    FOCAL_STRAIN_TABLE = args.focal_strain_table
    CONTEXT_STRAIN_TABLE = args.context_strain_table
    FOCAL_SAMPLE_NAME_COL = args.focal_sample_name_col
    CONTEXT_SAMPLE_NAME_COL = args.context_sample_name_col
    FOCAL_SEQ_COL = args.focal_seq_col
    CONTEXT_SEQ_COL = args.context_seq_col
    CONFIG_FILEPATH = args.config_filepath

    if args.automated:
        with open(CONFIG_FILEPATH, 'r') as stream:
            try:
                config_yaml = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        server_config = config_yaml['default']['database']['server']
        DB_NAME = server_config["dbname"]
        DB_HOST = server_config["host"]
        DB_USER = server_config["username"]
        DB_PASSWORD = server_config["password"]
    else:
        DB_NAME = args.dbname
        DB_HOST = args.dbhost
        DB_USER = args.username
        DB_PASSWORD = args.password
    
    # Connect to database
    db_connection = "dbname=\'" + DB_NAME + \
                    "\' user=\'" + DB_USER + \
                    "\' host=\'" + DB_HOST + \
                    "\' password=\'" + DB_PASSWORD + "\'"
    try:
        conn = psycopg2.connect(db_connection)
    except Exception as e:
        raise Exception("I am unable to connect to the database.", e)

    # Load strain lists
    focal_df = pd.read_csv(FOCAL_LIST, dtype={'strain': str})  # cast to string so that integer sample names work
    focal_strains = tuple(focal_df['strain'])

    context_df = pd.read_csv(CONTEXT_LIST)
    context_strains = tuple(context_df['strain'])

    # Query aligned sequences from database
    focal_seq_tuples = get_alignment(conn=conn, strains=focal_strains,
                                     table=FOCAL_STRAIN_TABLE,
                                     sample_name_col=FOCAL_SAMPLE_NAME_COL,
                                     seq_col=FOCAL_SEQ_COL)
    context_seq_tuples = get_alignment(conn=conn, strains=context_strains,
                                       table=CONTEXT_STRAIN_TABLE,
                                       sample_name_col=CONTEXT_SAMPLE_NAME_COL,
                                       seq_col=CONTEXT_SEQ_COL)

    # load entire alignment and the alignment of focal sequences (upper case -- probably not necessary)
    ref = sequence_to_int_array(SeqIO.read(REFERENCE, 'fasta').seq)
    focal_seqs_dict = calculate_snp_matrix(seq_tuples=focal_seq_tuples, consensus=ref)
    context_seqs_dict = calculate_snp_matrix(seq_tuples=context_seq_tuples, consensus=ref)
    alignment_length = len(ref)
    print("Done querying the aligned sequences.")

    # calculate number of masked sites in either set
    mask_count_focal = np.array([len(x) for x in focal_seqs_dict['filled_positions']])
    mask_count_context = {s: len(x) for s,x in zip(context_seqs_dict['names'], context_seqs_dict['filled_positions'])}

    # for each context sequence, calculate minimal distance to focal set, weigh with number of N/- to pick best sequence
    d = np.array(calculate_distance_matrix(context_seqs_dict['snps'], focal_seqs_dict['snps'], consensus = context_seqs_dict['consensus']))
    closest_match = np.argmin(d+mask_count_focal/alignment_length, axis=1)
    print("Done finding closest matches.")

    minimal_distance_to_focal_set = {}
    for context_index, focal_index in enumerate(closest_match):
        minimal_distance_to_focal_set[context_seqs_dict['names'][context_index]] = (d[context_index, focal_index], focal_seqs_dict["names"][focal_index])

    # for each focal sequence with close matches (using the index), we list all close contexts
    close_matches = defaultdict(list)
    for seq in minimal_distance_to_focal_set:
        close_matches[minimal_distance_to_focal_set[seq][1]].append(seq)

    for f in close_matches:
        shuffle(close_matches[f])
        close_matches[f].sort(key=lambda x: minimal_distance_to_focal_set[x][0] + mask_count_context[x]/alignment_length)

    # export priorities
    with open(OUTFILE, 'w') as fh:
        for i, seqid in enumerate(context_seqs_dict['names']):
            # use distance as negative priority
            # penalize masked (N or -) -- 333 masked sites==one mutations
            # penalize if many sequences are close to the same focal one by using the index of the shuffled list of neighbours
            # currently each position in this lists reduced priority by 0.2, i.e. 5 other sequences == one mutation
            position = close_matches[minimal_distance_to_focal_set[seqid][1]].index(seqid)
            priority = -minimal_distance_to_focal_set[seqid][0] - 0.1*position
            seqid_focal = minimal_distance_to_focal_set[seqid][1]
            fh.write(f"{seqid}\t{priority:1.2f}\t{seqid_focal}\n")


