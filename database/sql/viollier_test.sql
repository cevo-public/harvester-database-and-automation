-- Number samples per zip code and canton
select
  zip_code,
  canton,
  count(*) as number_samples
from viollier_test
where
  zip_code is not null
  and canton is not null
group by zip_code, canton;


-- Which zip codes are matched to more than one canton? (-> Probably errors?)
select zip_code, count(*)
from (
  select zip_code, canton, count(*)
  from viollier_test
  where zip_code is not null and canton is not null
  group by zip_code, canton
) x
group by zip_code
having count(*) > 1;


-- Find tests with multiple plates/wells
select vtvp.sample_number, count(*)
from viollier_test__viollier_plate vtvp
group by vtvp.sample_number
having count(*) > 1
order by count(*) desc;


-- Assign new ETHID to the samples designated for sequencing
with new_ethid as (
  select <first id> + row_number() over () as ethid, vt.sample_number
  from
    viollier_test vt
    join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
    join viollier_plate vp on vp.viollier_plate_name = vtvp.viollier_plate_name
  where vp.left_viollier_date = '<date>' and vp.gfb_number and vt.is_positive
)
update viollier_test vt
set ethid = ne.ethid
from new_ethid ne
where
  vt.sample_number = ne.sample_number
  and vt.ethid is null;

-- Prepare data for FGCZ
select
    order_date,
    ethid,
    fgcz_name,
    e_gene_ct,
    rdrp_gene_ct,
    viollier_plate_name,
    well_position,
    test_comment,
    plate_comment
from (
    select
      vt.sample_number,
      vp.left_viollier_date,
      vt.sequenced_by_viollier,
      vt.order_date,
      vt.ethid,
      vp.fgcz_name,
      vtvp.e_gene_ct,
      vtvp.rdrp_gene_ct,
      vtvp.viollier_plate_name,
      vtvp.well_position,
      vt.comment as test_comment,
      vp.comment as plate_comment,
      row_number() over (partition by ethid order by vtvp.viollier_plate_name like '%eg%' desc) as plate_priority -- if we sent multiple plates with the sample, prefer to sequence from eg plate
    from viollier_test vt
    join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
    join viollier_plate vp on vp.viollier_plate_name = vtvp.viollier_plate_name
    where
       left_viollier_date in ('2021-04-19', '2021-05-03')  -- order plate priority across plates sent to all centers on this date
       and is_positive
    ) test_w_priority
where
    plate_priority = 1
    and fgcz_name is not null  -- make this list for only plates sent to this center
    and not sequenced_by_viollier
    and not exists(
    select *
    from viollier_test__viollier_plate vtvp2
      join viollier_plate vp2 on vtvp2.viollier_plate_name = vp2.viollier_plate_name
    where vtvp2.sample_number = test_w_priority.sample_number
      and vp2.left_viollier_date < test_w_priority.left_viollier_date
    )  -- also don't include sample_numbers on plates that left viollier on an earlier date
order by
    (substring(fgcz_name, '[0-9]*$')::int - 1) % 7,
    substring(fgcz_name, '[0-9]*$')::int;

-- Update samples for which we requested sequencing at FGCZ
with tmp as(
  select *
  from
    (values
      ('210421eg4','E2')
      -- ...
    ) as t (viollier_plate_name, well_position)
)
update viollier_test__viollier_plate vtvp
set
  seq_request = TRUE
from tmp t
where
  t.viollier_plate_name = vtvp.viollier_plate_name
  and t.well_position = vtvp.well_position;


-- Prepare data for GFB
select
  zip_code,
  city,
  canton,
  order_date,
  sample_number,
  pcr_code,
  e_gene_ct,
  rdrp_gene_ct,
  viollier_plate_name,
  well_position,
  ethid,
  gfb_number,
  sample_comment,
  plate_comment
from (
    select
      vt.zip_code,
      vt.city,
      vt.canton,
      vt.order_date,
      vt.sample_number,
      vt.pcr_code,
      vtvp.e_gene_ct,
      vtvp.rdrp_gene_ct,
      vtvp.viollier_plate_name,
      vtvp.well_position,
      vt.ethid,
      vp.gfb_number,
      vt.comment as sample_comment,
      vp.comment as plate_comment,
      vp.left_viollier_date,
      vt.sequenced_by_viollier,
      row_number() over (partition by ethid order by vtvp.viollier_plate_name like '%eg%' desc) as plate_priority -- if we sent multiple plates with the sample, prefer to sequence from eg plate
    from
      viollier_test vt
      join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
      join viollier_plate vp on vp.viollier_plate_name = vtvp.viollier_plate_name
      left join bag_meldeformular bm on vt.sample_number = bm.sample_number
    where
       left_viollier_date in ('2021-04-19', '2021-05-03')  -- order plate priority across plates sent to all centers on this date
       and is_positive
    ) test_w_priority
where
  plate_priority = 1
  and gfb_number is not null  -- make this list for only plates sent to this center
  and not sequenced_by_viollier
  and not exists(
    select *
    from viollier_test__viollier_plate vtvp2
      join viollier_plate vp2 on vtvp2.viollier_plate_name = vp2.viollier_plate_name
    where vtvp2.sample_number = test_w_priority.sample_number
      and vp2.left_viollier_date < test_w_priority.left_viollier_date
  )  -- also don't include sample_numbers on plates that left viollier on an earlier date
order by gfb_number;

-- Update samples for which we requested sequencing at GFB
with tmp as(
  select *
  from
    (values
      ('210421eg4','E2')
      -- ...
    ) as t (viollier_plate_name, well_position)
)
update viollier_test__viollier_plate vtvp
set
  seq_request = TRUE
from tmp t
where
  t.viollier_plate_name = vtvp.viollier_plate_name
  and t.well_position = vtvp.well_position;


-- Prepare data for Health 2030
select
  order_date,
  ethid,
  e_gene_ct,
  rdrp_gene_ct,
  viollier_plate_name,
  well_position,
  test_comment,
  plate_comment
from (
    select
      vt.order_date,
      vt.ethid,
      vtvp.e_gene_ct,
      vtvp.rdrp_gene_ct,
      vtvp.viollier_plate_name,
      vtvp.well_position,
      vt.comment as test_comment,
      vp.comment as plate_comment,
      vt.sequenced_by_viollier,
      vt.sample_number,
      vp.left_viollier_date,
      vp.health2030,
      row_number() over (partition by ethid order by vtvp.viollier_plate_name like '%eg%' desc) as plate_priority -- if we sent multiple plates with the sample, prefer to sequence from eg plate
    from
      viollier_test vt
      join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
      join viollier_plate vp on vp.viollier_plate_name = vtvp.viollier_plate_name
    where
       left_viollier_date in ('2021-04-19', '2021-05-03')  -- order plate priority across plates sent to all centers on this date
       and is_positive
    ) test_w_priority
where
  plate_priority = 1
  and health2030
  and not test_w_priority.sequenced_by_viollier
  and not exists(
    select *
    from viollier_test__viollier_plate vtvp2
      join viollier_plate vp2 on vtvp2.viollier_plate_name = vp2.viollier_plate_name
    where vtvp2.sample_number = test_w_priority.sample_number
      and vp2.left_viollier_date < test_w_priority.left_viollier_date
  )  -- also don't include sample_numbers on plates that left viollier on an earlier date
order by test_w_priority.viollier_plate_name desc, test_w_priority.well_position;

-- Update samples for which we requested sequencing at H2030
with tmp as(
  select *
  from
    (values
      ('210421eg4','E2')
      -- ...
    ) as t (viollier_plate_name, well_position)
)
update viollier_test__viollier_plate vtvp
set
  seq_request = TRUE
from tmp t
where
  t.viollier_plate_name = vtvp.viollier_plate_name
  and t.well_position = vtvp.well_position;


-- Take a look at the order clause :)
select
  vt.order_date,
  vt.ethid,
  vp.fgcz_name,
  vtvp.e_gene_ct,
  vtvp.rdrp_gene_ct,
  vtvp.viollier_plate_name,
  vtvp.well_position,
  vt.comment as test_comment,
  vp.comment as plate_comment,
  coalesce(vt.canton, '') = 'VS' as sample_is_from_vs,
  (case
     when exists( -- If the plate contains a sample from VS, it should be prioritized
       select *
       from viollier_test vt2
              join viollier_test__viollier_plate vtvp2 on vt2.sample_number = vtvp2.sample_number
       where vt2.is_positive
         and vt2.canton = 'VS'
         and vtvp2.viollier_plate_name = vtvp.viollier_plate_name
       ) then true
     else false
    end) as plate_contains_vs_sample,
  order_date > '2020-12-11' as plate_is_new
from
  viollier_test vt
  join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
  join viollier_plate vp on vp.viollier_plate_name = vtvp.viollier_plate_name
where
  vp.left_viollier_date = '2020-12-21'
  and vp.fgcz_name is not null
  and vt.is_positive
order by
  sample_is_from_vs desc,
  coalesce(vt.comment = 'The patient came back from South Africa.', false) desc,
  plate_is_new desc,
  plate_contains_vs_sample desc,
  vtvp.viollier_plate_name desc;


-- Look up by sample number
select
  x.sample_number,
  vt.ethid,
  vt.order_date,
  vp.sequencing_center,
  vp.left_viollier_date,
  vtvp.viollier_plate_name,
  vtvp.well_position,
  vt.comment,
  vp.*,
  si.gisaid_id,
  coalesce(si.spsp_uploaded_at, si.gisaid_uploaded_at) as uploaded_at
from
  (values
    (12345678)
  ) as x (sample_number)
  left join viollier_test vt on x.sample_number = vt.sample_number
  left join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
  left join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
  left join consensus_sequence cs on vt.ethid = cs.ethid
  left join sequence_identifier si on cs.sample_name = si.sample_name;
