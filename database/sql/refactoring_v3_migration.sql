-- Migrating the "normal" samples coming from Viollier
insert into z_test_metadata (test_id, ethid, order_date, zip_code, canton, city, is_positive, purpose, comment)
select
  'viollier/' || sample_number,
  ethid,
  order_date,
  zip_code,
  canton,
  city,
  is_positive,
  purpose,
  comment
from viollier_test;


-- Migrating the samples from the non_viollier_test table:
-- The table contains mostly Team-w samples, but also a few samples from other other labs, including Viollier.
-- The Viollier samples in the table do not have a sample number. We disconnected the sequences from their
-- sample numbers because we are not sure about their mapping.
-- The following helper query shows the labs in the non_viollier_test table and whether a sample number is known.
--     select
--       covv_orig_lab,
--       sample_number is not null,
--       count(*)
--     from
--       non_viollier_test
--     group by
--       covv_orig_lab,
--       sample_number is not null;
-- The results:
--     Viollier AG,false,69
--     "Clinical Laboratory, Vetsuisse Faculty, University of Zurich",false,8
--     MCL Medizinische Laboratorien Hauptstandort Niederwangen,true,4
--     "Institute for Molecular Health Science, ETH Zurich",false,48
--     Institut Central des Hôpitaux Valaisans ICHV/ZIWS Service des Maladies Infectieuses,false,1
--     "Department of Dermatology, University of Zürich, University of Zürich Hospital, Switzerland ",false,48
--     labor team w AG,true,3814
-- For the samples without a known sample number, we will "unknown"
insert into z_test_metadata (test_id, ethid, order_date, zip_code, canton, city, is_positive, purpose, comment)
select
  (case -- For the test id, I think that it is better to use a short name for the labs
    when covv_orig_lab = 'Viollier AG' then 'viollier'
    when covv_orig_lab = 'Clinical Laboratory, Vetsuisse Faculty, University of Zurich' then 'vetsuisse'
    when covv_orig_lab = 'MCL Medizinische Laboratorien Hauptstandort Niederwangen' then 'mcl_niederwangen'
    when covv_orig_lab = 'Institute for Molecular Health Science, ETH Zurich' then 'mhs_eth'
    when covv_orig_lab = 'Department of Dermatology, University of Zürich, University of Zürich Hospital, Switzerland '
      then 'dermatology_uzh'
    when 'labor team w AG' then 'team_w'
  end) || '/' ||
  coalesce(sample_number, 'unknown'),
  si.ethid,
  nvt.order_date,
  nvt.zip_code,
  nvt.canton,
  null,
  true,
  null,
  nvt.comment
from
  non_viollier_test nvt
  left join sequence_identifier si on nvt.sample_name = si.sample_name;


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


-- After 1 August 2021, the new procedure where plates with only positive samples are sent to the sequencing
-- labs is running smoothly. I.e., the samples from a "viollier plate" will also be sequenced together.
-- For the samples before that, we don't have this information. If we want to fill the table for those samples, we need
-- to ask the sequencing labs.
insert into z_sequencing_plate (sequencing_plate_name, sequencing_center)
select viollier_plate_name, sequencing_center
from viollier_plate vp
where left_viollier_date >= '2021-08-01';


-- Before 7 June 2021, we had the old procedure. Samples resulted that it shall be marked as "old".
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
