drop view bag_sequence_report;
create view bag_sequence_report
as select
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
    from
(
    select
        *, row_number() over (partition by ethid order by gisaid_id, consensus_n) as priority_idx
        from
    (
        select
            vt.order_date as viol_sample_date,
            cs.ethid,
            consensus_n,
            coverage as mean_coverage,
            gisaid_id,
            case
                when vt.sample_number is not null then vt.sample_number
                when nvt.sample_number is not null then nvt.sample_number
            end as auftraggeber_nummer,
            nvt.sample_name as alt_seq_id,
            case
                when cs.comment like '%reinfection case%' then 'outbreak'
                when vt.comment like '%ausbruch%' then 'outbreak'
                when vt.comment like '%Ausbruch%' then 'outbreak'
                when vt.comment like '%outbreak%' then 'outbreak'
                when vt.comment like '%Outbreak%' then 'outbreak'
                when vt.comment like '%Weitere Fälle%' then 'outbreak'
                when cs.comment like '%travel case%' then 'travel case'
                when vt.comment like '%travel case%' then 'travel case'
                when bm.comment like 'auftraggeber_armee=TRUE' then 'screening'
                when vt.sample_number is not null then 'surveillance'
                end as viro_purpose,
            case
                when bm.comment like 'auftraggeber_armee=TRUE' then 'Armee'
                when nvt.covv_orig_lab is not null then nvt.covv_orig_lab
                when vt.sample_number is not null then 'Swiss SARS-CoV-2 Sequencing Consortium'
                end as viro_source,
            case
                when cs.sample_name is not null then 'ETHZ, D-BSSE'
                end as viro_seq,
            case
                when cs.sample_name is not null then 'wgs'
                end as viro_characterised,
            si.gisaid_id as viro_gisaid_id,
            null as viro_genbank_id,
            case
                when cs.sample_name is not null then 'MN908947.3'
                end as viro_ref_sequence_id,
            case
                when nc.pangolin_lineage <> 'None' then nc.pangolin_lineage
                end as viro_label,
            viro_relevant_mutations_to_ref_seq
        from consensus_sequence cs
        left join non_viollier_test nvt on nvt.sample_name = cs.sample_name
        left join viollier_test vt on vt.ethid = cs.ethid
        left join bag_meldeformular bm on vt.sample_number = bm.sample_number
        left join sequence_identifier si on cs.ethid = si.ethid
        left join (
            select
               sample_name,
               string_agg(aa_mutation, ', ') viro_relevant_mutations_to_ref_seq
            from consensus_sequence_nextclade_mutation_aa csnma
            GROUP BY sample_name) t2 on cs.sample_name = t2.sample_name
        left join consensus_sequence_nextclade_data nc on cs.sample_name = nc.sample_name
    ) tbl_w_duplicates
    where ethid is not null) tbl_w_duplicate_priority
where priority_idx = 1;
