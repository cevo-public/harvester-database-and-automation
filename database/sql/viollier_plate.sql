-- Find plates that don't have positive samples from Basel and Zurich and that are already at GFB
select *
from viollier_plate vp
where
  vp.gfb_number is not null
  and not exists(
    select *
    from
      viollier_test vt
      join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
    where
      vp.viollier_plate_name = vtvp.viollier_plate_name
      and (vt.canton is null or vt.canton in ('BS', 'BL', 'ZH'))
  );


-- Check whether there are multiple samples assigned to the same plate+well.
select vtvp.viollier_plate_name, vtvp.well_position, string_agg(vt.sample_number::text, ',')
from
  viollier_test__viollier_plate vtvp
  join viollier_test vt on vtvp.sample_number = vt.sample_number
where vt.order_date >= '<date>'
group by vtvp.viollier_plate_name, vtvp.well_position
having count(*) > 1;


-- Get the number of positive samples per plate and sorts the results by order date and by the plate name in a useful
-- way.
select
  vp.viollier_plate_name,
  substring(vp.viollier_plate_name from 5 for 2) as year,
  substring(vp.viollier_plate_name from 3 for 2) as month,
  substring(vp.viollier_plate_name for 2) as day,
  regexp_replace(vp.viollier_plate_name, '[0-9]*', '', 'g') as plate_name_part2,
  regexp_replace(vp.viollier_plate_name, '.*[a-zA-Z]+', '0')::integer as plate_name_part3,
  count(*) as count
from
  viollier_plate vp
  join viollier_test__viollier_plate vtvp on vp.viollier_plate_name = vtvp.viollier_plate_name
  join viollier_test vt on vtvp.sample_number = vt.sample_number
where
  vt.order_date >= '<date>'
  and vt.is_positive
  and regexp_replace(vp.viollier_plate_name, '[0-9]*', '', 'g') = 'eg'
  and vp.left_viollier_date is null
  and not vt.sequenced_by_viollier
group by
  vp.viollier_plate_name
order by
  year,
  month,
  day,
  plate_name_part2,
  plate_name_part3;


-- Update plates that were sent to FGCZ
with tmp as(
  select *
  from
    (values
      ('123456ab78','p12345_78901/0001')
      -- ...
    ) as t (viollier_plate_name, fgcz_name)
)
update viollier_plate vp
set
  fgcz_name = t.fgcz_name,
  left_viollier_date = '<date>'
from tmp t
where
  t.viollier_plate_name = vp.viollier_plate_name
  and vp.fgcz_name is null
  and vp.left_viollier_date is null;


-- Update plates that were sent to GFB
with tmp as(
  select *
  from
    (values
      ('123456ab78','9999')
      -- ...
    ) as t (viollier_plate_name, gfb_number)
)
update viollier_plate vp
set
  gfb_number = t.gfb_number,
  left_viollier_date = '<date>'
from tmp t
where
  t.viollier_plate_name = vp.viollier_plate_name
  and vp.gfb_number is null
  and vp.left_viollier_date is null;


-- Update plates that were sent to health2030
with tmp as(
  select *
  from
    (values
      ('123456ab78',true)
      -- ...
    ) as t (viollier_plate_name, health2030)
)
update viollier_plate vp
set
  health2030 = t.health2030,
  left_viollier_date = '<date>'
from tmp t
where
  t.viollier_plate_name = vp.viollier_plate_name
  and vp.health2030 is null
  and vp.left_viollier_date is null;


-- Get some statistics about the types/names of plates. How many samples are on *eg* or *wuhan*
-- (Biorad PCR machines?) plates?
select
  extract(year from vt.order_date) order_year,
  extract(month from vt.order_date) order_month,
  sum(case when viollier_plate_name like '%eg%' then 1 else 0 end) number_samples_on_eg_plates,
  sum(case when viollier_plate_name like '%wuhan%' then 1 else 0 end) number_samples_on_wuhan_plates,
  sum(case
    when viollier_plate_name not like '%eg%'
           and viollier_plate_name not like '%wuhan%' then 1 else 0
    end) number_samples_on_other_plates -- e.g., og, tb etc.
from
  viollier_test vt
  join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
where vt.is_positive
group by order_year, order_month;


-- Find plates that has wells that are assigned to more than one sample. They indicate labelling errors!!
-- It should be avoided to process them. If they get sequenced, the results have to be checked very carefully.
select *
from
  (
    select viollier_plate_name
    from
      (
        select vtvp.viollier_plate_name, vtvp.well_position, count(*)
        from viollier_test__viollier_plate vtvp
          join viollier_test vt on vtvp.sample_number = vt.sample_number
        where vt.order_date >= '<date>'
        group by vtvp.viollier_plate_name, vtvp.well_position
        having count(*) > 1
      ) x
    group by viollier_plate_name
  ) x
  join viollier_plate vp on x.viollier_plate_name = vp.viollier_plate_name
order by left_viollier_date, gfb_number, fgcz_name, health2030;


-- Find plates containing army samples
select
  vp.viollier_plate_name,
  substring(vp.viollier_plate_name from 5 for 2) as year,
  substring(vp.viollier_plate_name from 3 for 2) as month,
  substring(vp.viollier_plate_name for 2) as day,
  regexp_replace(vp.viollier_plate_name, '[0-9]*', '', 'g') as plate_name_part2,
  regexp_replace(vp.viollier_plate_name, '.*[a-zA-Z]+', '0')::integer as plate_name_part3,
  count(*) as count
from
  bag_meldeformular bm
  join viollier_test vt on bm.sample_number = vt.sample_number
  join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
  join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
where
  bm.comment like '%auftraggeber_armee=TRUE%'
  and vt.is_positive
  and vt.order_date >= '<date>'
group by
  vp.viollier_plate_name
order by
  year,
  month,
  day,
  plate_name_part2,
  plate_name_part3;


-- Summarize the sequencing results and status for the plates after June 7 2021.
create view viollier_plate_statistics_after_607 as
select
  vp.viollier_plate_name,
  vp.sequencing_center,
  vp.left_viollier_date as approx_extraction_date,
  count(*) as number_samples,
  sum(case when vtvp.e_gene_ct < 32 then 1 else 0 end) as ct_below_32,
  sum(case when vtvp.e_gene_ct >= 32 then 1 else 0 end) as ct_at_least_32,
  sum(case when cs.sample_name is not null then 1 else 0 end) as sequenced,
  sum(case when cs.fail_reason = 'no fail reason' then 1 else 0 end) as sequenced_success,
  sum(case when cs.fail_reason = 'no fail reason' and vtvp.e_gene_ct < 32 then 1 else 0 end)
    as sequenced_ct_below_32_success,
  sum(case when cs.fail_reason = 'no fail reason' and vtvp.e_gene_ct >= 32 then 1 else 0 end)
    as sequenced_ct_at_least_32_success,
  sum(case when coalesce(si.gisaid_uploaded_at, si.spsp_uploaded_at) is not null then 1 else 0 end)
    as gisaid_submitted,
  sum(case when gisaid_id is not null then 1 else 0 end) as gisaid_id_available,
  string_agg(case when gisaid_id is not null then vt.sample_number::text end, ',') as gisaid_id_available_sample_number,
  string_agg(case when cs.fail_reason <> 'no fail reason' and vtvp.e_gene_ct < 32 then vt.sample_number::text end, ',')
    as sequenced_failed_ct_below_32_sample_number,
  string_agg(distinct cs.sequencing_batch, ',') as sequencing_batch
from
  viollier_plate vp
  join viollier_test__viollier_plate vtvp on vp.viollier_plate_name = vtvp.viollier_plate_name
  join viollier_test vt on vtvp.sample_number = vt.sample_number
  left join consensus_sequence cs
      on vt.ethid = cs.ethid and vp.sequencing_center = cs.sequencing_center
  left join sequence_identifier si on cs.sample_name = si.sample_name
where
  -- This is the date when we were at Viollier to relabel plates for the last time.
  vp.left_viollier_date > '2021-06-07'
  and vt.is_positive -- do we get data from negative individuals? should we flag these to alert Viollier something went wrong in the cherry picking?
group by
  vp.viollier_plate_name,
  vp.sequencing_center,
  vp.left_viollier_date
order by vp.left_viollier_date desc;

-- Find if samples from a plate have been sequenced
select
    vp.viollier_plate_name,
    left_viollier_date,
    vp.sequencing_center,
    fgcz_name, gfb_number,
    vtvp.sample_number,
    cs.sample_name is not null as has_sequence,
    vt.sample_number is not null as has_metadata
from
    viollier_plate vp
    join viollier_test__viollier_plate vtvp on vp.viollier_plate_name = vtvp.viollier_plate_name
    left join consensus_sequence cs on vp.sequencing_center = cs.sequencing_center and vtvp.sample_number = cs.ethid
    left join viollier_test vt on vtvp.sample_number = vt.sample_number
where
    vp.viollier_plate_name = '<missing>';

select * from bag_meldeformular where sample_number = 33504383;

select *
from viollier_test__viollier_plate
left join viollier_plate on viollier_test__viollier_plate.viollier_plate_name = viollier_plate.viollier_plate_name
where fgcz_name = '<missing>';

select
    *
from
     consensus_sequence
where
    sample_name like 'NA_%';