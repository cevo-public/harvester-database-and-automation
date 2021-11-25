-- ##### General statistics #####

-- How many of the samples that left Viollier have been sequenced?
select
  vp.left_viollier_date,
  vp.sequencing_center,
  count(*) as left_viollier,
  sum(case when cs.seq is not null then 1 else 0 end) as sequenced
from
  (
    select
      vp.*,
      (case
        when gfb_number is not null then 'gfb'
        when fgcz_name is not null then 'fgcz'
        when health2030 is not null then 'h2030'
      end) as sequencing_center
    from viollier_plate vp
  ) vp
  join viollier_test__viollier_plate vtvp on vp.viollier_plate_name = vtvp.viollier_plate_name
  join viollier_test vt on vtvp.sample_number = vt.sample_number
  left join consensus_sequence cs on vt.ethid = cs.ethid and vp.sequencing_center = cs.sequencing_center
where
  vt.is_positive
  and vp.left_viollier_date is not null
group by vp.left_viollier_date, vp.sequencing_center
order by vp.left_viollier_date desc, vp.sequencing_center;


-- Sequencing batch statistics
select
  sequencing_batch,
  sequencing_center,
  sum(case when fail_reason = 'no fail reason' then 1 else 0 end) as good_quality,
  sum(case when fail_reason like '%frameshift%' then 1 else 0 end) as possible_frameshift,
  sum(case when fail_reason <> 'no fail reason' and fail_reason not like '%frameshift%' then 1 else 0 end) as other_problems,
  count(*) as total,
  round(sum(case when "fail_reason" = 'no fail reason' then 1 else 0 end) * 1.0 / count(*), 2) as good_quality_proportion
from consensus_sequence
group by
  sequencing_batch,
  sequencing_center
order by
  sequencing_batch desc;

-- Nextclade is finished
select count(*)
from consensus_sequence cs
where not exists(
  select
  from consensus_sequence_nextclade_data nd
  where cs.sample_name = nd.sample_name
);

-- After Nextclade.. Pangolin lineage is also finished
select count(*)
from consensus_sequence_nextclade_data nd
where pangolin_lineage is null;


-- Average coverage of the "good" sequences per lab
select
  sequencing_center,
  avg(coverage)
from
  consensus_sequence cs
  join viollier_test vt on cs.ethid = vt.ethid
where
  vt.order_date >= '2021-01-01'
  and fail_reason = 'no fail reason'
group by sequencing_center;



-- ##### Analyzing a selected batch #####

-- Fail reasons
select
  fail_reason,
  count(*)
from consensus_sequence
where sequencing_batch = ''
group by rollup (fail_reason);

-- Inspect sample names
select sample_name, ethid, coverage
from consensus_sequence
where sequencing_batch = '';

-- How many plates & samples per plates does the batch contain?
select
    viollier_plate_name,
    count(*) as n_seqs
from consensus_sequence cs
left join viollier_test vt on cs.ethid = vt.ethid
left join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
where sequencing_batch = ''
group by viollier_plate_name;

-- Does it look like the frameshift indel reports got generated, imported for samples in this batch?
select
count (distinct fdd.sample_name) as n_samples_with_a_frameshift_indel_reported,
count (distinct cs.sample_name) as n_samples
from consensus_sequence cs
left join frameshift_deletion_diagnostic fdd on cs.sample_name = fdd.sample_name
where sequencing_batch = '';

