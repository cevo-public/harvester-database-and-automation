
create table z_test_metadata (
  test_id text primary key,
  ethid integer unique,
  order_date date,
  zip_code text,
  canton text,
  city text,
  is_positive boolean not null,
  purpose text,
  comment text
);


create table z_extraction_plate (
  extraction_plate_name text primary key,
  gfb_number text,
  fgcz_name text,
  health2030 boolean,
  left_lab_or_received_metadata_date date,
  sequencing_center text,
  viollier_extract_free boolean,
  comment text
);


create table z_sequencing_plate (
  sequencing_plate_name text primary key,
  sequencing_center text not null,
  sequencing_date date,
  comment text
);


create table z_test_plate_mapping (
  test_id text
    references z_test_metadata (test_id) on update cascade on delete set null,
  old_sample boolean default false not null,
  extraction_plate text
    references z_extraction_plate (extraction_plate_name) on update cascade on delete restrict,
  extraction_plate_well text,
  extraction_e_gene_ct integer,
  extraction_rdrp_gene_ct integer,
  sequencing_plate text
    references z_sequencing_plate (sequencing_plate_name) on update cascade on delete restrict,
  sequencing_plate_well text,
  sample_type text
);

create unique index on z_test_plate_mapping (extraction_plate, extraction_plate_well) where not old_sample;
create unique index on z_test_plate_mapping (sequencing_plate, sequencing_plate_well) where  not old_sample;



-- We keep the ETHID and sequencing_center for the old samples for which we don't know the plate.
create table z_consensus_sequence (
  sample_name text primary key,
  sequencing_plate text,
  sequencing_plate_well text,
  insert_date timestamp,
  update_date timestamp,
  sequencing_center text,
  sequencing_batch text,
  seq_aligned text,
  seq_unaligned text,
  ethid integer
);


create table z_consensus_sequence_meta (
  sample_name text primary key
    references z_consensus_sequence (sample_name) on update cascade on delete cascade,

  -- Our own QC (V-pipe)
  coverage_mean double precision,
  r1_basequal text,
  r2_basequal text,
  rejreads double precision,
  alnreads double precision,
  insertsize integer,
  consensus_n integer,
  qc_result text,

  -- Nextstrain diagnostic.py
  diagnostic_divergence integer,
  diagnostic_excess_divergence double precision,
  diagnostic_number_n integer,
  diagnostic_number_gaps integer,
  diagnostic_clusters text,
  diagnostic_gaps text,
  diagnostic_all_snps text,
  diagnostic_flagging_reason text,

  -- Nextclade
  nextclade_clade text,
  nextclade_qc_overall_score double precision,
  nextclade_qc_overall_status text,
  nextclade_total_gaps integer,
  nextclade_total_insertions integer,
  nextclade_total_missing integer,
  nextclade_total_mutations integer,
  nextclade_total_non_acgtns integer,
  nextclade_total_pcr_primer_changes integer,
  nextclade_alignment_start integer,
  nextclade_alignment_end integer,
  nextclade_alignment_score integer,
  nextclade_qc_missing_data_score double precision,
  nextclade_qc_missing_data_status text,
  nextclade_qc_missing_data_total integer,
  nextclade_qc_mixed_sites_score double precision,
  nextclade_qc_mixed_sites_status text,
  nextclade_qc_mixed_sites_total integer,
  nextclade_qc_private_mutations_cutoff integer,
  nextclade_qc_private_mutations_excess integer,
  nextclade_qc_private_mutations_score double precision,
  nextclade_qc_private_mutations_status text,
  nextclade_qc_private_mutations_total integer,
  nextclade_qc_snp_clusters_clustered text,
  nextclade_qc_snp_clusters_score double precision,
  nextclade_qc_snp_clusters_status text,
  nextclade_qc_snp_clusters_total integer,
  nextclade_errors text,

  -- PANGO lineage
  pango_lineage text,
  pango_probability double precision,
  pango_learn_version text,
  pango_status text,
  pango_note text
);


create table z_consensus_sequence_mutation_aa (
  sample_name text not null
    references z_consensus_sequence (sample_name) on update cascade on delete cascade,
  aa_mutation text not null,
  primary key (sample_name, aa_mutation)
);


create table z_consensus_sequence_mutation_nucleotide (
  sample_name text not null
    references z_consensus_sequence (sample_name) on update cascade on delete cascade,
  nuc_mutation text not null,
  primary key (sample_name, nuc_mutation)
);


create table z_consensus_sequence_notes (
  sample_name text primary key
    references z_consensus_sequence (sample_name) on update cascade on delete cascade,
  release_decision boolean,
  comment text
);
