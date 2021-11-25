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
