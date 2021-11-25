-- Institutional ranking
select max(submitting_lab) as lab,
       string_agg(distinct country, '; ') as country,
       count(*) as number_submitted_sequences
from gisaid_api_sequence g
where country = 'CHE'
group by lower(submitting_lab)
order by count(*) desc;


-- Country ranking
select
  country_original,
  count(*)
from gisaid_api_sequence g
group by country_original
order by count(*) desc;


-- Author ranking
select
  trim(a.author) as author,
  count(*) as number_sequences
from
  gisaid_api_sequence gs,
  unnest(string_to_array(gs.authors, ',')) a(author)
where
  gs.authors is not null
--   and gs.country = 'CHE'
group by trim(a.author)
order by number_sequences desc;


-- First author ranking
select
  split_part(gs.authors, ',', 1) as first_author,
  count(*) as number_sequences
from gisaid_api_sequence gs
where
  gs.authors is not null
--   and gs.country = 'CHE'
group by split_part(gs.authors, ',', 1)
order by number_sequences desc;


-- Time distribution
select extract(isoyear from date_submitted),
       extract(week from date_submitted),
       count(*)
from gisaid_api_sequence g
where country = 'CHE'
group by extract(isoyear from date_submitted),
         extract(week from date_submitted)
order by extract(isoyear from date_submitted),
         extract(week from date_submitted);


-- Our delay
select
  extract(year from date) as year,
  extract(month from date) as month,
  avg(date_submitted - date)
from gisaid_api_sequence
where submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zürich'
group by year, month
order by year, month;

-- Our weekly coverage of confirmed cases; week 53 wraps around I guess?
select
    *,
    round(n_sequences::numeric * 100 / n_positive_tests::numeric, 1) as percent_tests_sequenced
from (
   select
       extract(year from date) as year,
       extract(week from date) as week,
       count(*) as n_sequences
    from gisaid_api_sequence
    where submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zürich'
    group by year, week
) s full join (
    select
       extract(year from date) as year,
       extract(week from date) as week,
       sum(positive_tests) as n_positive_tests
    from bag_test_numbers
    group by year, week
) t on s.year = t.year and s.week = t.week;

-- OUTDATED AND UNMAINTAINED: Find out number B117 per week in the UK
select
  'UK',
  m.year, m.week,
  (select count(*)
  from gisaid_sequence g
  where g.country = 'United Kingdom'
    and extract(isoyear from g.date) = m.year and extract(week from g.date) = m.week) as n,
  count(*) as b117
from
  (
    select
      cs.strain,
      extract(isoyear from date) as year,
      extract(week from date) as week,
      date,
      count(*) as number_of_present_mutations,
      count(*) * 1.0 / (select count(*) from variant_mutation_aa where variant_name = 'B.1.1.7') as proportion,
      string_agg(csnm.aa_mutation, ',') as mutations
    from
      gisaid_sequence cs
      join gisaid_sequence_nextclade_mutation_aa csnm on cs.strain = csnm.strain
      join variant_mutation_aa vm on csnm.aa_mutation = vm.aa_mutation
    where vm.variant_name = 'B.1.1.7' and cs.country = 'United Kingdom'
    group by cs.strain
    having count(*) * 1.0 / (select count(*) from variant_mutation_aa where variant_name = 'B.1.1.7') >= 0.8
  ) m
where year is not null
group by
  m.year, m.week;


-- The sequencing coverage from other labs
with sequenced as (
  select
    extract(year from gs.date) as year,
    extract(month from gs.date) as month,
    count(*) as sequenced
  from gisaid_api_sequence gs
  where
    gs.country = 'CHE'
    and gs.submitting_lab <> 'Department of Biosystems Science and Engineering, ETH Zürich'
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
  join sequenced s on c.year = s.year and c.month = s.month
order by year, month;


-- Frequencies of hosts
select host, count(*)
from gisaid_api_sequence
group by host
order by count(*) desc;


-- The frequency of the bases
select
  unnest(regexp_split_to_array(upper(s.seq_original), '')) as base,
  count(*)
from gisaid_api_sequence s
group by base
order by base;


-- OUTDATED AND UNMAINTAINED: Find the number of B.1.1.7 mismatch between our 80% variant caller and Nextstrain
with sequences as (
  select *
  from gisaid_sequence cs
  where
    submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zürich'
    and cs.date >= '2020-12-14'
),
our_variant_calling as (
  select
    x.date as date,
    sum(case
      when number_confirmed_mutations + number_confirmed_original + number_weird >=
            0.8 * (select count(*) from variant_mutation_nucleotide where variant_name = 'B.1.1.7')
          then 1 else 0
    end) as sequenced,
    sum(case
      when number_confirmed_mutations >=
            0.8 * (select count(*) from variant_mutation_nucleotide where variant_name = 'B.1.1.7')
          then 1 else 0
    end) as b117
  from
    (
      select
        x.strain as sample_name,
        x.division as division,
        x.date,
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
                substr(x.aligned_seq, x.position, 1) as seq_base,
                x.*
              from
                (
                  select
                    gs.strain,
                    gs.division,
                    gs.date,
                    gs.aligned_seq,
                    vmn.nucleotide_mutation,
                    vmn.corresponding_aa_mutation,
                    substr(vmn.nucleotide_mutation, 1, 1) as original,
                    substr(vmn.nucleotide_mutation, 2, char_length(vmn.nucleotide_mutation) - 2)::int as position,
                    substr(vmn.nucleotide_mutation, char_length(vmn.nucleotide_mutation), 1) as mutated
                  from sequences gs,
                    variant_mutation_nucleotide vmn
                  where vmn.variant_name = 'B.1.1.7'
                ) x
            ) x
        ) x
      group by x.strain, x.division, x.date
    ) x
  group by x.date
),
nextclade as (
  select
    s.date,
    count(*) as sequenced,
    sum(case when s.nextstrain_clade = '20I/501Y.V1' then 1 else 0 end) as b117
  from sequences s
  group by s.date
)
select
  o.date,
  o.sequenced as sequenced,
  o.b117 as our_variant_calling_b117,
  n.b117 as nextclade_b117,
  o.b117 - n.b117 as mismatch
from
  our_variant_calling o
  join nextclade n on o.date = n.date
order by o.date;
