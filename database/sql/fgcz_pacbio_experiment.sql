-- The pangolin lineages within a sequencing batch among GISAID-accepted sequences
select
  cs.sequencing_batch,
  gs.pangolin_lineage,
  count(*)
from
  consensus_sequence cs
  join sequence_identifier si on cs.ethid = si.ethid
  join gisaid_sequence gs on si.gisaid_id = gs.gisaid_epi_isl
where
  sequencing_center = 'fgcz'
  and sequencing_batch = '20210205_HWL33DRXX'
group by
  cs.sequencing_batch,
  gs.pangolin_lineage
order by cs.sequencing_batch desc, gs.pangolin_lineage;


-- The sample list sent to FGCZ
select
  vt.order_date,
  vt.ethid,
  vp.fgcz_name,
  vtvp.e_gene_ct,
  vtvp.rdrp_gene_ct,
  vtvp.viollier_plate_name,
  vtvp.well_position,
  gs.pangolin_lineage
from
  consensus_sequence cs
  join sequence_identifier si on cs.ethid = si.ethid
  join gisaid_sequence gs on si.gisaid_id = gs.gisaid_epi_isl
  join viollier_test vt on cs.ethid = vt.ethid
  join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
  join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
where
  cs.sequencing_center = 'fgcz'
  and vp.fgcz_name is not null
  and cs.sequencing_batch = '20210205_HWL33DRXX'
order by fgcz_name;

-- The data returned by PacBio (normalized and not normalized)
select *
from consensus_sequence
where sequencing_batch = 'PacBioTestBatch';

-- Create view to match sample_name to seq_methods or relevant samples
create view pacbio_test as
    select
        cs1.sample_name,
        case
            when cs1.sample_name like 'pacbioTestNormalized_%' then 'pacbioTestNormalized'
            when cs1.sample_name like 'pacbioTest_%' then 'pacbioTestNotNormalized'
            else 'illumina'
        end as seq_method,
        case
            when notes.ethid is null then cs1.ethid
            when cs1.ethid is null then notes.ethid
        end as ethid
    from consensus_sequence cs1
    left join x_consensus_sequence_notes notes on cs1.sample_name = notes.sample_name
    where exists(
         select *
         from consensus_sequence cs2
                  left join x_consensus_sequence_notes notes on cs2.sample_name = notes.sample_name
         where cs2.sequencing_batch = 'PacBioTestBatch'
           and cs1.ethid = notes.ethid
     )
    or sequencing_batch = 'PacBioTestBatch';

-- Compare overall pass rates between normalized and non-normalized pacbio, illumina consensus sequences
select
    seq_method,
    fail_reason,
    count(*) as n_seqs
from consensus_sequence cs
right join pacbio_test pt on cs.sample_name = pt.sample_name
group by seq_method, fail_reason;

-- Insertions - expected only in pacbio consensuses
-- check if lineage B.1.214.2 sequences now have expected insertion & no/few many insertions in other lineage sequences
-- PacBio Not Normalized has some spurious insertions (n = 3) -- bad
-- PacBio Normalized has insertions for all B.1.214.2 sequences (n = 4) and B.1.617.1 (n = 1) and only these samples -- perfect
select
    pt.ethid,
    string_agg(pangolin_lineage, ', ' order by seq_method) as lineage_assignments,
    string_agg(nextclade_total_insertions::text, ', ' order by seq_method) as insertions,
    string_agg(seq_method, ', ' order by seq_method) as seq_methods,
    sum(nextclade_total_insertions) > 0 as some_method_has_insertion,
    pt.ethid = 560325 as delta_variant
from consensus_sequence cs
right join pacbio_test pt on cs.sample_name = pt.sample_name
left join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name
group by pt.ethid
order by some_method_has_insertion desc;

-- Deletions - no changes to how these are called beyond some PacBio normalization?
-- check # unique deletions, common deletions across the 2 consensuses, same proportion of deletions introduce frameshifts?
-- gaps only filled for Illumina right now
select
    ethid,
    gap,
    string_agg(seq_method, ', ') as methods_yielding_gap
from (
     select pt.ethid,
            seq_method,
            regexp_split_to_table(gaps, ',') as gap
     from consensus_sequence cs
              right join pacbio_test pt on cs.sample_name = pt.sample_name
     ) as gap_data
group by ethid, gap;

-- Substitutions - no changes to how these are called beyond some PacBio normalization?
-- check # unique subs, common subs across the 2 consensuses

-- check consistency in lineage assignments





