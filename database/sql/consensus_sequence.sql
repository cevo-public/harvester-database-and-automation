-- Get the Nextclade mutations on a sequence that corresponds to a selected variant based on amino acid changes
-- Change "aa" to "nucleotide" to find mutations on the DNA-level.
select
  cs.sample_name,
  count(*) as number_of_present_mutations,
  count(*) * 1.0 / (select count(*) from variant_mutation_aa where variant_name = '<variant name>') as proportion,
  string_agg(csnm.aa_mutation, ',') as mutations
from
  consensus_sequence cs
  join consensus_sequence_nextclade_mutation_aa csnm on cs.sample_name = csnm.sample_name
  join variant_mutation_aa vm on csnm.aa_mutation = vm.aa_mutation
where vm.variant_name = '<variant name>'
group by cs.sample_name
order by count(*) desc;


-- Compute the mutations by ourselves
-- It reports the confirmed mutations (✔), unknown i.e. consensus sequence report a N (?), and
-- if there is a mutation at the same position but not the one for the variant (!).
-- This query might take up to a few minutes.
select
  x.sample_name,
  sum(case
    when mutation_state = '✔' then 1
    else 0
  end) as confirmed_mutations,
  sum(case
    when mutation_state = '?' or mutation_state = '!' then 1
    else 0
  end) as possible_mutation,
  string_agg(
    x.nucleotide_mutation || ' (' || x.corresponding_aa_mutation || ') ' || mutation_state,
    ', '
  ) as mutations
from
  (
    select
      x.sample_name,
      x.nucleotide_mutation,
      x.corresponding_aa_mutation,
      (case
        when upper(base) = upper(original) then '⨯'
        when upper(base) = upper(mutation) then '✔'
        when upper(base) = 'N' then '?'
        else '!'
      end) as mutation_state
    from
      (
        select
          cs.sample_name,
          cs.seq,
          vmn.nucleotide_mutation,
          vmn.corresponding_aa_mutation,
          substr(vmn.nucleotide_mutation, 1, 1) as original,
          substr(vmn.nucleotide_mutation, 2, char_length(vmn.nucleotide_mutation) - 2)::int as position,
          substr(vmn.nucleotide_mutation, char_length(vmn.nucleotide_mutation), 1) as mutation,
          substr(cs.seq, substr(vmn.nucleotide_mutation, 2, char_length(vmn.nucleotide_mutation) - 2)::int, 1) as base
        from
          consensus_sequence cs,
          variant_mutation_nucleotide vmn
        where variant_name = '<variant name>'
      ) x
    order by x.sample_name, x.position
  ) x
where x.mutation_state <> '⨯'
group by x.sample_name
having
  sum(case
    when mutation_state = '✔' then 1
    else 0
  end) > 0
order by confirmed_mutations desc, possible_mutation desc;


-- Get the number of randomly sampled sequences of a variant per week. A sequence is defined as being of a variant if it
-- has 80% of the mutations. It also computes the number of sequences "n" for which we could call a variant (i.e.,
-- enough bases are known).
select
  extract(isoyear from vt.order_date) as year,
  extract(week from vt.order_date) as week,
  sum(case
    when number_confirmed_mutations >=
          0.8 * (select count(*) from variant_mutation_nucleotide where variant_name = '<variant name>')
        then 1 else 0
  end) as confirmed,
  sum(case
    when number_confirmed_mutations + number_confirmed_original + number_weird >=
          0.8 * (select count(*) from variant_mutation_nucleotide where variant_name = '<variant name>')
        then 1 else 0
  end) as n
from
  (
    select
      x.sample_name,
      x.ethid,
      sum(case when x.mutation_state = '+' then 1 else 0 end) as number_confirmed_mutations,
      sum(case when x.mutation_state = '-' then 1 else 0 end) as number_confirmed_original,
      sum(case when x.mutation_state = '?' then 1 else 0 end) as number_unknowns,
      sum(case when x.mutation_state = '!' then 1 else 0 end) as number_weird
    from
      (
        select
          (case
            when upper(seq_base) = upper(original) then '-'
            when upper(seq_base) = upper(mutated) then '+'
            when upper(seq_base) = 'N' then '?'
            else '!'
          end) as mutation_state,
          x.*
        from
          (
            select
              substr(x.seq, x.position, 1) as seq_base,
              x.*
            from
              (
                select
                  cs.sample_name,
                  cs.ethid,
                  cs.seq,
                  vmn.nucleotide_mutation,
                  vmn.corresponding_aa_mutation,
                  substr(vmn.nucleotide_mutation, 1, 1) as original,
                  substr(vmn.nucleotide_mutation, 2, char_length(vmn.nucleotide_mutation) - 2)::int as position,
                  substr(vmn.nucleotide_mutation, char_length(vmn.nucleotide_mutation), 1) as mutated
                from
                  (select * from consensus_sequence cs where is_random or is_random is null) cs,
                  variant_mutation_nucleotide vmn
                where vmn.variant_name = '<variant name>'
              ) x
          ) x
      ) x
    group by x.sample_name, x.ethid
  ) x
  join viollier_test vt on x.ethid = vt.ethid
group by year, week
order by year, week;


-- Get our sequencing coverage: Compute the proportions of sequences to the overall number of cases in Switzerland
with sequenced as (
  select
    extract(year from vt.order_date) as year,
    extract(month from vt.order_date) as month,
    count(*) as sequenced
  from
    consensus_sequence cs
    join viollier_test vt on cs.ethid = vt.ethid
  group by year, month
),
cases as (
  select
      extract(year from bdm.fall_dt) as year,
      extract(month from bdm.fall_dt) as month,
      count(*) as cases
  from bag_dashboard_meldeformular bdm
  group by year, month
)
select
  s.year,
  s.month,
  c.cases,
  s.sequenced,
  round(s.sequenced * 100.0 / c.cases, 2) || '%' as coverage
from
  cases c
  join sequenced s on c.year = s.year and c.month = s.month;


-- A detailed statistics about the number of sequenced samples per lab through time
-- Please note that (processed_by_gfb + processed_by_fgcz + processed_by_h2030) could be larger than left_viollier and
-- received_sequencing_results because some samples are on multiple plates and could have been sequenced more than once.
-- The left_viollier column is unknown for before October 2020.
with all_positives as (
  select vt.sample_number, vt.ethid, vt.order_date
  from
    viollier_test vt
  where
    vt.order_date >= '2020-01-01'
    and vt.is_positive
),
left_viollier as (
  select *
  from all_positives a
  where
    exists(
      select *
      from viollier_test__viollier_plate vtvp
        join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
      where a.sample_number = vtvp.sample_number
        and vp.left_viollier_date is not null
    )
),
gfb as (
  select *
  from all_positives a
  where
    exists(
      select *
      from viollier_test__viollier_plate vtvp
        join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
      where a.sample_number = vtvp.sample_number
        and vp.gfb_number is not null
    )
),
fgcz as (
  select *
  from all_positives a
  where
    exists(
      select *
      from viollier_test__viollier_plate vtvp
        join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
      where a.sample_number = vtvp.sample_number
        and vp.fgcz_name is not null
    )
),
h2030 as (
  select *
  from all_positives a
  where
    exists(
      select *
      from viollier_test__viollier_plate vtvp
        join viollier_plate vp on vtvp.viollier_plate_name = vp.viollier_plate_name
      where a.sample_number = vtvp.sample_number
        and vp.health2030
    )
),
received_sequencing_results as (
  select *
  from all_positives a
  where
    exists(
      select *
      from consensus_sequence cs
      where cs.ethid = a.ethid
    )
),
gisaid_id_is_known as (
  select *
  from all_positives a
  where
    exists(
      select *
      from sequence_identifier si
      where
        a.ethid = si.ethid
        and si.gisaid_id is not null
    )
)
select
  x1.order_date,
  x1.all_positives,
  coalesce(x2.left_viollier, 0) as left_viollier,
  coalesce(x21.processed_by_gfb, 0) as processed_by_gfb,
  coalesce(x22.processed_by_fgcz, 0) as processed_by_fgcz,
  coalesce(x23.processed_by_h2030, 0) as processed_by_h2030,
  coalesce(x3.received_sequencing_results, 0) as received_sequencing_results,
  coalesce(x4.gisaid_id_is_known, 0) as gisaid_id_is_known
from
  (select order_date, count(*) as all_positives from all_positives group by order_date) x1
  left join (select order_date, count(*) as left_viollier from left_viollier group by order_date) x2 on x1.order_date = x2.order_date
  left join (select order_date, count(*) as processed_by_gfb from gfb group by order_date) x21 on x1.order_date = x21.order_date
  left join (select order_date, count(*) as processed_by_fgcz from fgcz group by order_date) x22 on x1.order_date = x22.order_date
  left join (select order_date, count(*) as processed_by_h2030 from h2030 group by order_date) x23 on x1.order_date = x23.order_date
  left join (select order_date, count(*) as received_sequencing_results from received_sequencing_results group by order_date) x3 on x1.order_date = x3.order_date
  left join (select order_date, count(*) as gisaid_id_is_known from gisaid_id_is_known group by order_date) x4 on x1.order_date = x4.order_date
order by x1.order_date;


-- How many samples were sequenced per week (order_date) and sequencing center and what's the GISAID upload status?
select
  x.*,
  ((x.gisaid_submitted_but_not_confirmed + gisaid_confirmed_upload) * 1.0 / sequenced) as submitted_rate
from
  (
    select
      extract(isoyear from vt.order_date) as year,
      extract(week from vt.order_date) as week,
      cs.sequencing_center,
      count(*) as sequenced,
      sum(case when cs.no_fail_reason then 1 else 0 end) as submittable,
      sum(case when si.ethid is not null and si.gisaid_id is null then 1 else 0 end) as gisaid_submitted_but_not_confirmed,
      sum(case when si.gisaid_id is not null then 1 else 0 end) as gisaid_confirmed_upload
    from
      viollier_test vt
      join (
        select
          ethid,
          fail_reason = 'no fail reason' as no_fail_reason,
          sequencing_center
        from consensus_sequence cs
        where
          not exists(
            select
            from consensus_sequence cs2
            where
              cs.ethid = cs2.ethid
              and (
                (cs.fail_reason <> 'no fail reason' and cs2.fail_reason = 'no fail reason') -- If there is a sequence that did not fail, take that.
                or (cs.fail_reason = cs2.fail_reason and cs2.number_n < cs.number_n) -- If multiple sequences have the same fail status, take the one with less unknown bases.
                or (cs.fail_reason = cs2.fail_reason and cs2.number_n = cs.number_n and cs2.sample_name < cs.sample_name) -- If both also have the same number of unknowns, take the one with "smaller" name
              )
          )
      ) cs on vt.ethid = cs.ethid
      left join sequence_identifier si on vt.ethid = si.ethid
    group by year, week, cs.sequencing_center
    order by year, week, cs.sequencing_center
  ) x;


-- Format as fasta
select string_agg('>' || sample_number || E'\n' || seq || E'\n', E'\n')
from
  consensus_sequence cs
  join viollier_test vt on cs.ethid = vt.ethid
where vt.sample_number in (<sample numbers>);
