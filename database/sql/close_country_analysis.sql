
with relevant_sequences as (
  select g.strain
  from
    gisaid_sequence g
--     join gisaid_sequence_nextclade_mutation_aa m on g.strain = m.strain
--     join variant_mutation_aa v on m.aa_mutation = v.aa_mutation
  where
    g.country = 'USA'
    and extract(year from g.date) = 2020 and extract(month from g.date) = 3
--     and v.variant_name = 'B.1.1.7'
--   group by g.strain
--   having count(*) >= 0.8 * (select count(*) from variant_mutation_aa where variant_name = 'B.1.1.7')
),
est_country as (
  select
    ec.strain,
    ec.close_country as country,
    count(*) as count
  from
    relevant_sequences r
    join gisaid_sequence_close_country ec on r.strain = ec.strain
  group by ec.strain, ec.close_country
),
absolute_majority as (
  select ec.strain, ec.country
  from est_country ec
  where ec.count >= 6
)
select
  bg.country,
  round((count(*) * 1.0 / (select count(*) from absolute_majority) * 100), 2) || '%' as percentage,
  count(*) as count
from
  absolute_majority bg
group by rollup(bg.country)
order by count desc;


with relevant_sequences as (
  select g.strain, g.date
  from
    gisaid_sequence g
--     join gisaid_sequence_nextclade_mutation_aa m on g.strain = m.strain
--     join variant_mutation_aa v on m.aa_mutation = v.aa_mutation
  where
    g.country = 'Denmark'
--     and v.variant_name = 'B.1.1.7'
--   group by g.strain
--   having count(*) >= 0.8 * (select count(*) from variant_mutation_aa where variant_name = 'B.1.1.7')
),
est_country as (
  select
    ec.strain,
    r.date,
    ec.close_country as country,
    count(*) as count
  from
    relevant_sequences r
    join gisaid_sequence_close_country ec on r.strain = ec.strain
  group by ec.strain, r.date, ec.close_country
),
absolute_majority as (
  select ec.strain, ec.country, ec.date
  from est_country ec
  where ec.count >= 6
),
tmp as (
  select
    bg.country,
    extract(year from bg.date) as year,
    extract(month from date) as month,
    count(*) as count
  from
    absolute_majority bg
  group by bg.country, extract(year from bg.date), extract(month from date)
)
select
  t.country,
  t.year,
  t.month,
  t.count,
  t.count*1.0/t2.sum as proportion
from
  tmp t
  join (
    select year, month, sum(count) as sum
    from tmp
    group by year, month
  ) t2 on t.year = t2.year and t.month = t2.month
order by t.year, t.month, t.country