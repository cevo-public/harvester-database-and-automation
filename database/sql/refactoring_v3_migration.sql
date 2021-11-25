
insert into z_test_metadata (test_id, ethid, order_date, zip_code, canton, city, is_positive, comment)
select
  'viollier/' || sample_number,
  ethid,
  order_date,
  zip_code,
  canton,
  city,
  is_positive,
  comment
from viollier_test;

-- TODO merge non_viollier_plate into test_sample

insert into z_extraction_plate (
  extraction_plate_name, gfb_number, fgcz_name, health2030, left_lab_or_received_metadata_date,
  sequencing_center, viollier_extract_free, comment
)
select
  viollier_plate_name,
  gfb_number,
  fgcz_name,
  health2030,
  left_viollier_date,
  sequencing_center,
  has_no_extract,
  comment
from viollier_plate;


insert into z_sequencing_plate (sequencing_plate_name, sequencing_center)
select viollier_plate_name, sequencing_center
from viollier_plate vp
where left_viollier_date >= '2021-08-01';


insert into z_test_plate_mapping (
  test_id, extraction_plate, extraction_plate_well, extraction_e_gene_ct, extraction_rdrp_gene_ct,
  sample_type, old_sample
)
select
  'viollier/' || vtvp.sample_number,
  vtvp.viollier_plate_name,
  vtvp.well_position,
  vtvp.e_gene_ct,
  vtvp.rdrp_gene_ct,
  'clinical',
  true
from
  viollier_test__viollier_plate vtvp
  join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
where vp.left_viollier_date <= '2021-06-07';

insert into z_test_plate_mapping (
  test_id, extraction_plate, extraction_plate_well, extraction_e_gene_ct, extraction_rdrp_gene_ct,
  sample_type, old_sample
)
select
  'viollier/' || vtvp.sample_number,
  vtvp.viollier_plate_name,
  vtvp.well_position,
  vtvp.e_gene_ct,
  vtvp.rdrp_gene_ct,
  'clinical',
  false
from
  viollier_test__viollier_plate vtvp
  join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
where vp.left_viollier_date > '2021-06-07';

insert into z_consensus_sequence (
  sample_name, sequencing_center, sequencing_batch, seq_aligned, ethid
)
select
  sample_name,
  sequencing_center,
  sequencing_batch,
  seq,
  ethid
from consensus_sequence;

insert into z_consensus_sequence_meta (
  sample_name,

  coverage_mean, coverage_median, r1_basequal, r2_basequal, rejreads, alnreads, insertsize, consensus_n,
  consensus_lcbases, consensus_diffbases, snvs, snvs_majority, qc_result,

  diagnostic_divergence, diagnostic_excess_divergence, diagnostic_number_n, diagnostic_number_gaps,
  diagnostic_clusters, diagnostic_gaps, diagnostic_all_snps, diagnostic_flagging_reason,

  nextclade_clade, nextclade_qc_overall_score, nextclade_qc_overall_status, nextclade_total_gaps,
  nextclade_total_insertions, nextclade_total_missing, nextclade_total_mutations, nextclade_total_non_acgtns,
  nextclade_total_pcr_primer_changes, nextclade_alignment_start, nextclade_alignment_end,
  nextclade_alignment_score, nextclade_qc_missing_data_score, nextclade_qc_missing_data_status,
  nextclade_qc_missing_data_total, nextclade_qc_mixed_sites_score, nextclade_qc_mixed_sites_status,
  nextclade_qc_mixed_sites_total, nextclade_qc_private_mutations_cutoff, nextclade_qc_private_mutations_excess,
  nextclade_qc_private_mutations_score, nextclade_qc_private_mutations_status,
  nextclade_qc_private_mutations_total, nextclade_qc_snp_clusters_clustered, nextclade_qc_snp_clusters_score,
  nextclade_qc_snp_clusters_status, nextclade_qc_snp_clusters_total, nextclade_errors,

  pango_lineage, pango_probability, pango_learn_version, pango_status, pango_note
)
select
  cs.sample_name,

  coverage, null, r1_basequal, r2_basequal, rejreads, alnreads, insertsize, consensus_n,
       consensus_lcbases, diffb


from
  consensus_sequence cs
  left join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name
