-- Hospitalization and death counts for B.1.1.7 vs. wildtype
with unique_cs as (
  select
    cs.*,
    sum(case when m.aa_mutation is not null then 1 else 0 end) >=
      0.8 * (select count(*) from variant_mutation_aa where variant_name = 'B.1.1.7') as b117
  from
    consensus_sequence cs
    left join (
      select cm.*
      from
        consensus_sequence_nextclade_mutation_aa cm
        join variant_mutation_aa vm on cm.aa_mutation = vm.aa_mutation
      and vm.variant_name = 'B.1.1.7'
    ) m on cs.sample_name = m.sample_name
  where
    not exists(
      select
      from consensus_sequence cs2
      where
        cs.ethid = cs2.ethid
        and (cs2.consensus_n < cs.consensus_n or (cs2.consensus_n = cs.consensus_n and cs2.sample_name < cs.sample_name))
    )
  group by cs.sample_name
),
samples as (
  select
    vt.sample_number,
    vt.ethid,
    cs.b117 as b117,
    bm.altersjahr,
    coalesce(bm.hospitalisation_type = 'HOSPITALIZED', false) as hospitalized,
    coalesce(bm.pttod, false) as dead
  from
    viollier_test vt
    join unique_cs cs on vt.ethid = cs.ethid
    join bag_meldeformular bm on vt.sample_number = bm.sample_number
  where vt.order_date between '2021-01-01' and '2021-02-15'
)
select
  (case
    when s.altersjahr < 10 then '0-9'
    when s.altersjahr between 10 and 19 then '10-19'
    when s.altersjahr between 20 and 29 then '20-29'
    when s.altersjahr between 30 and 39 then '30-39'
    when s.altersjahr between 40 and 49 then '40-49'
    when s.altersjahr between 50 and 59 then '50-59'
    when s.altersjahr between 60 and 69 then '60-69'
    when s.altersjahr between 70 and 79 then '70-79'
    when s.altersjahr >= 80 then '80+'
  end) as age_group,
  count(*) as total,
  sum(case when s.hospitalized and b117 then 1 else 0 end) as b117_hospitalized,
  sum(case when not s.hospitalized and b117 then 1 else 0 end) as b117_not_hospitalized,
  sum(case when s.hospitalized and not b117 then 1 else 0 end) as not_b117_hospitalized,
  sum(case when not s.hospitalized and not b117 then 1 else 0 end) as not_b117_not_hospitalized,
  sum(case when s.dead and b117 then 1 else 0 end) as b117_dead,
  sum(case when not s.dead and b117 then 1 else 0 end) as b117_not_dead,
  sum(case when s.dead and not b117 then 1 else 0 end) as not_b117_dead,
  sum(case when not s.dead and not b117 then 1 else 0 end) as not_b117_not_dead
from samples s
group by age_group;


select
  (case
    when s.altersjahr < 10 then '0-9'
    when s.altersjahr between 10 and 19 then '10-19'
    when s.altersjahr between 20 and 29 then '20-29'
    when s.altersjahr between 30 and 39 then '30-39'
    when s.altersjahr between 40 and 49 then '40-49'
    when s.altersjahr between 50 and 59 then '50-59'
    when s.altersjahr between 60 and 69 then '60-69'
    when s.altersjahr between 70 and 79 then '70-79'
    when s.altersjahr >= 80 then '80+'
  end) as age_group,
  count(*) as total,
  sum(case when s.hospitalized and voc then 1 else 0 end) as voc_hospitalized,
  sum(case when not s.hospitalized and voc then 1 else 0 end) as voc_not_hospitalized,
  sum(case when s.hospitalized and not voc then 1 else 0 end) as not_voc_hospitalized,
  sum(case when not s.hospitalized and not voc then 1 else 0 end) as not_voc_not_hospitalized,
  sum(case when s.dead and voc then 1 else 0 end) as voc_dead,
  sum(case when not s.dead and voc then 1 else 0 end) as voc_not_dead,
  sum(case when s.dead and not voc then 1 else 0 end) as not_voc_dead,
  sum(case when not s.dead and not voc then 1 else 0 end) as not_voc_not_dead
from
  (
    select
      s.*,
      coalesce(s.hospitalisation = 1, false) as hospitalized,
      coalesce(s.pttod, false) as dead,
      coalesce(s.confirmed_variant_of_concern_txt = '1', false) as voc
    from bag_dashboard_meldeformular s
    where s.fall_dt between '2021-01-01' and '2021-02-15'
  ) s
group by age_group;
