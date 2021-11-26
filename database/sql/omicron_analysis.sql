-- Check if Omicron is in our data
select
  vt.*,
  cs.sample_name,
  cs.sequencing_center,
  si.gisaid_id,
  nd.pangolin_lineage
from
  (
    (
      select m.sample_name
      from
        consensus_sequence_nextclade_mutation_aa m
      where m.aa_mutation = 'S:A67V' or m.aa_mutation = 'S:G339D'
      group by m.sample_name
      having count(*) = 2
    )
    union
    (
      select nd.sample_name
      from consensus_sequence_nextclade_data nd
      where nd.pangolin_lineage = 'B.1.1.529'
    )
  ) m
  left join consensus_sequence cs on m.sample_name = cs.sample_name
  left join viollier_test vt on cs.ethid = vt.ethid
  left join sequence_identifier si on cs.sample_name = si.sample_name
  left join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name;
