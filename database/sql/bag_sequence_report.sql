drop view bag_sequence_report;
create view bag_sequence_report as
select *
from
  (( -- For the samples processed by the "new procedure"
    select
      tm.test_sample_number as auftraggeber_nummer,
      -- The sample_name column from the old non_viollier_test is not planned to be continued
      -- in the new schema. Therefore, we won't fill alt_seq_id. I think that we are already
      -- providing sufficient information to uniquely identify an entry.
      null as alt_seq_id,
      tm.purpose as viro_purpose,
      case
        when bm.comment like 'auftraggeber_armee=TRUE' then 'Armee'
        when tm.test_lab = 'viollier' then 'Swiss SARS-CoV-2 Sequencing Consortium'
        -- TODO add other labs
      end as viro_source,
      'ETHZ, D-BSSE' as viro_seq,
      'wgs' as viro_characterised,
      si.gisaid_id as viro_gisaid_id,
      null as viro_genbank_id,
      'MN908947.3' as viro_ref_sequence_id,
      csma.viro_relevant_mutations_to_ref_seq,
      case
        when csm.pango_lineage <> 'None' then csm.pango_lineage
      end as viro_label,
      tm.order_date as viol_sample_date,
      csm.consensus_n,
      csm.coverage_mean as mean_coverage
    from
      z_consensus_sequence cs
      join z_consensus_sequence_meta csm on cs.sample_name = csm.sample_name
      join z_test_plate_mapping m
        on cs.sequencing_plate = m.sequencing_plate
             and cs.sequencing_plate_well = m.sequencing_plate_well
      left join (
        select
          *,
          split_part(test_id, '/', 2) as test_lab,
          split_part(test_id, '/', 2) as test_sample_number
        from z_test_metadata
      ) tm on m.test_id = tm.test_id
      left join bag_meldeformular bm on tm.test_sample_number = bm.sample_number::text
      left join sequence_identifier si on tm.ethid = si.ethid
      left join (
        select
          sample_name,
          string_agg(aa_mutation, ', ') viro_relevant_mutations_to_ref_seq
        from z_consensus_sequence_mutation_aa
        group by sample_name
      ) csma on cs.sample_name = csma.sample_name
    where
      cs.sequencing_plate is not null
  ) union all
  ( -- For the old samples: We only use ETHID to join if the sequencing_plate is not known
    select
      auftraggeber_nummer,
      alt_seq_id,
      viro_purpose,
      viro_source,
      viro_seq,
      viro_characterised,
      viro_gisaid_id,
      viro_genbank_id,
      viro_ref_sequence_id,
      viro_relevant_mutations_to_ref_seq,
      viro_label,
      viol_sample_date,
      consensus_n,
      mean_coverage
    from (
      select
            row_number() over (partition by tm.ethid order by gisaid_id, consensus_n) as priority_idx,
            tm.test_sample_number as auftraggeber_nummer,
            -- The sample_name column from the old non_viollier_test is not planned to be continued
            -- in the new schema. Therefore, we won't fill alt_seq_id. I think that we are already
            -- providing sufficient information to uniquely identify an entry.
            null as alt_seq_id,
            tm.purpose as viro_purpose,
            case
              when bm.comment like 'auftraggeber_armee=TRUE' then 'Armee'
              when tm.test_lab = 'viollier' then 'Swiss SARS-CoV-2 Sequencing Consortium'
              -- TODO add other labs
            end as viro_source,
            'ETHZ, D-BSSE' as viro_seq,
            'wgs' as viro_characterised,
            si.gisaid_id as viro_gisaid_id,
            null as viro_genbank_id,
            'MN908947.3' as viro_ref_sequence_id,
            csma.viro_relevant_mutations_to_ref_seq,
            case
              when csm.pango_lineage <> 'None' then csm.pango_lineage
            end as viro_label,
            tm.order_date as viol_sample_date,
            csm.consensus_n,
            csm.coverage_mean as mean_coverage
          from
            z_consensus_sequence cs
            join z_consensus_sequence_meta csm on cs.sample_name = csm.sample_name
            left join (
              select
                *,
                split_part(test_id, '/', 2) as test_lab,
                split_part(test_id, '/', 2) as test_sample_number
              from z_test_metadata
            ) tm on cs.ethid = tm.ethid
            left join test_plate_mapping m on tm.test_id = m.test_id
            left join bag_meldeformular bm on tm.test_sample_number = bm.sample_number::text
            left join sequence_identifier si on tm.ethid = si.ethid
            left join (
              select
                sample_name,
                string_agg(aa_mutation, ', ') viro_relevant_mutations_to_ref_seq
              from z_consensus_sequence_mutation_aa
              group by sample_name
            ) csma on cs.sample_name = csma.sample_name
          where
            cs.sequencing_plate is null
    ) x
    where priority_idx = 1
  )) x
order by x;
