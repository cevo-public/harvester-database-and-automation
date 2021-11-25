-- Check ETHID
--     Find sequences with unexpected strain names
select gs.strain, regexp_replace(gs.strain, '.*-|/.*', '', 'g')
from
  gisaid_api_sequence gs
  join sequence_identifier si on gs.gisaid_epi_isl = si.gisaid_id
where not regexp_replace(gs.strain, '.*-|/.*', '', 'g') ~ '^[0-9]*$';


--     Compare ETHID of sequences with parsable strain names
select count(*)
from
  gisaid_api_sequence gs
  join sequence_identifier si on gs.gisaid_epi_isl = si.gisaid_id
where
  regexp_replace(gs.strain, '.*-|/.*', '', 'g') ~ '^[0-9]*$'
  and regexp_replace(gs.strain, '.*-|/.*', '', 'g')::integer <> si.ethid;


-- Compare dates
select count(*)
from
  gisaid_api_sequence gs
  join sequence_identifier si on gs.gisaid_epi_isl = si.gisaid_id
  join consensus_sequence cs on cs.sample_name = si.sample_name
  join viollier_test vt on cs.ethid = vt.ethid
where vt.order_date <> gs.date;


-- Compare sequences
select
  count(*)
from
  gisaid_api_sequence gs
  join sequence_identifier si on gs.gisaid_epi_isl = si.gisaid_id
  join consensus_sequence cs on cs.sample_name = si.sample_name
where
  replace(lower(gs.seq_aligned), '-', 'n') <> replace(lower(cs.seq), '-', 'n');


-- Compare pangolin lineages
select
  si.*,
  nd.pangolin_lineage as our_pangolin_lineage,
  gs.pangolin_lineage as gisaid_pangolin_lineage
from
  gisaid_api_sequence gs
  join sequence_identifier si on gs.gisaid_epi_isl = si.gisaid_id
  join consensus_sequence cs on cs.sample_name = si.sample_name
  join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name
where gs.pangolin_lineage <> nd.pangolin_lineage
order by ethid desc;


-- Compare Nextclade clades

select
  si.*,
  nd.nextclade_clade as our_nextclade_clade,
  gs.nextclade_clade as gisaid_nextclade_clade
from
  gisaid_api_sequence gs
  join sequence_identifier si on gs.gisaid_epi_isl = si.gisaid_id
  join consensus_sequence cs on cs.sample_name = si.sample_name
  join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name
where gs.nextclade_clade <> nd.nextclade_clade
order by ethid desc;
