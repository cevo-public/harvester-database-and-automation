-- Drop everything

-- drop materialized view spectrum_sequence_intensity;
-- drop materialized view spectrum_pangolin_lineage_mutation_nucleotide;
-- drop materialized view spectrum_pangolin_lineage_mutation;
-- drop materialized view spectrum_sequence_private_meta;
-- drop materialized view spectrum_swiss_cases;
-- drop materialized view spectrum_sequence_public_mutation_nucleotide;
-- drop materialized view spectrum_sequence_public_mutation_aa;
-- drop materialized view spectrum_sequence_public_meta;


-- Delete cache

-- truncate spectrum_api_cache_sample;


-- Create materialized views

-- TODO Include non-Viollier sequences
-- Combines gisaid_api_sequence with unreleased consensus_sequence...
create materialized view staging_spectrum_sequence_public_meta as
select
  coalesce(si.gisaid_id, 'UNRELEASED_ETHZ_' || cs.ethid) as sequence_name,
  vt.order_date as date,
  'Europe' as region,
  'Switzerland' as country,
  coalesce(sc.english, 'Switzerland') as division,
  null as location,
  null as zip_code,
  'Human' as host,
  bm.altersjahr as age,
  (case when bm.sex = 'Männlich' then 'Male' when bm.sex = 'Weiblich' then 'Female' end) as sex,
  'Department of Biosystems Science and Engineering, ETH Zürich' as submitting_lab,
  'Viollier AG' as originating_lab,
  coalesce(hospitalisation_type = 'HOSPITALIZED', false) as hospitalized,
  coalesce(pttod, false) as deceased,
  (case when nd.pangolin_lineage <> 'None' then nd.pangolin_lineage end) as pangolin_lineage,
  null as vaccination_status,
  null as vaccination_doses,
  null as vaccination_first_dose_date,
  null as vaccination_second_dose_date
from
  (
    -- We take all sequences that are on GISAID
    select *
    from consensus_sequence cs
    where
      exists (
        select
        from sequence_identifier si
        where
          cs.sample_name = si.sample_name
          and si.gisaid_id is not null
      )
    union all
    select *
    from consensus_sequence cs
    where
      not exists(
        select
        from sequence_identifier si
        where
          (cs.sample_name = si.sample_name or cs.ethid = si.ethid)
          and si.gisaid_id is not null
      )
      and not exists(
        select *
        from consensus_sequence cs2
        where
          -- If there are two sequences with the same ETHID, take the one with the smaller number of n
          cs2.ethid = cs.ethid
          and (
            cs2.number_n < cs.number_n  -- Choose the sequence that has less n
            or (cs2.number_n = cs.number_n and cs2.sample_name < cs.sample_name)  -- the number of n is equal, just choose one based on name
          )
      )
    and cs.fail_reason = 'no fail reason'
  ) cs
  join viollier_test vt on cs.ethid = vt.ethid
  left join sequence_identifier si on vt.ethid = si.ethid
  left join swiss_canton sc on vt.canton = sc.canton_code
  left join bag_meldeformular bm on vt.sample_number = bm.sample_number
  left join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name
union all
select
  gs.gisaid_epi_isl as sequence_name,
  gs.date,
  sc.region,
  sc.name as country,
  gs.division,
  gs.location,
  null as zip_code,
  gs.host,
  gs.age,
  (case when gs.sex = 'Male' or gs.sex = 'Female' then gs.sex end) as sex,
  gs.submitting_lab,
  gs.originating_lab,
  null as hospitalized,
  null as deceased,
  (case when gs.pangolin_lineage <> 'None' then gs.pangolin_lineage end) as pangolin_lineage,
  null as vaccination_status,
  null as vaccination_doses,
  null as vaccination_first_dose_date,
  null as vaccination_second_dose_date
from
  gisaid_api_sequence gs
  join spectrum_country sc on gs.country = sc.iso_code
where
  -- Team-W will be included because it's not in viollier_test
  (gs.strain not like '%-ETHZ-%' or lower(gs.originating_lab) = lower('Labor Team W Ag'))
  and gs.host = 'Human'
  and gs.date >= '2020-01-01';

-- ... and the corresponding amino acid mutations
create materialized view staging_spectrum_sequence_public_mutation_aa as
select
  m.sequence_name,
  m.aa_mutation,
  split_part(lower(m.aa_mutation), ':', 1) as aa_mutation_gene,
  substr(split_part(lower(m.aa_mutation), ':', 2), 2, char_length(split_part(lower(m.aa_mutation), ':', 2)) - 2)::int as aa_mutation_position,
  substr(split_part(lower(m.aa_mutation), ':', 2), char_length(split_part(lower(m.aa_mutation), ':', 2)), 1) as aa_mutation_base
from
  staging_spectrum_sequence_public_meta s
  join (
    select
      gs.gisaid_epi_isl as sequence_name,
      gsnma.aa_mutation
    from
      gisaid_api_sequence gs
      join gisaid_api_sequence_nextclade_mutation_aa gsnma on gs.gisaid_epi_isl = gsnma.gisaid_epi_isl
    where (gs.strain not like '%-ETHZ-%' or lower(gs.originating_lab) = lower('Labor Team W Ag'))
    union all
    select
      coalesce(si.gisaid_id, 'UNRELEASED_ETHZ_' || cs.ethid) as sequence_name,
      csnma.aa_mutation
    from
      (
        -- We take all sequences that are on GISAID
        select *
        from consensus_sequence cs
        where
          exists (
            select
            from sequence_identifier si
            where
              cs.sample_name = si.sample_name
              and si.gisaid_id is not null
          )
        union all
        select cs.*
        from
          consensus_sequence cs
        where
          not exists(
            select
            from sequence_identifier si
            where
              (cs.sample_name = si.sample_name or cs.ethid = si.ethid)
              and si.gisaid_id is not null
          )
          and not exists(
            select *
            from consensus_sequence cs2
            where
              -- If there are two sequences with the same ETHID, take the one with the smaller number of n
              cs2.ethid = cs.ethid
              and (
                cs2.number_n < cs.number_n  -- Choose the sequence that has less n
                or (cs2.number_n = cs.number_n and cs2.sample_name < cs.sample_name)  -- the number of n is equal, just choose one based on name
              )
          )
        and cs.fail_reason = 'no fail reason'
      ) cs
      join viollier_test vt on cs.ethid = vt.ethid
      join consensus_sequence_nextclade_mutation_aa csnma on cs.sample_name = csnma.sample_name
      left join sequence_identifier si on cs.ethid = si.ethid
  ) m on s.sequence_name = m.sequence_name;


-- ... and the nucleotide mutations
create materialized view staging_spectrum_sequence_public_mutation_nucleotide as
select
  m.sequence_name,
  m.position,
  m.mutation as mutation_base
from
  staging_spectrum_sequence_public_meta s
  join (
    select
      gs.gisaid_epi_isl as sequence_name,
      gsmn.position,
      gsmn.mutation
    from
      gisaid_api_sequence gs
      join gisaid_api_sequence_mutation_nucleotide gsmn on gs.gisaid_epi_isl = gsmn.gisaid_epi_isl
    where (gs.strain not like '%-ETHZ-%' or lower(gs.originating_lab) = lower('Labor Team W Ag'))
    union all
    select
      coalesce(si.gisaid_id, 'UNRELEASED_ETHZ_' || cs.ethid) as sequence_name,
      csmn.position,
      csmn.mutation
    from
      (
        -- We take all sequences that are on GISAID
        select *
        from consensus_sequence cs
        where
          exists (
            select
            from sequence_identifier si
            where
              cs.sample_name = si.sample_name
              and si.gisaid_id is not null
          )
        union all
        select cs.*
        from
          consensus_sequence cs
        where
          not exists(
            select
            from sequence_identifier si
            where
              (cs.sample_name = si.sample_name or cs.ethid = si.ethid)
              and si.gisaid_id is not null
          )
          and not exists(
            select *
            from consensus_sequence cs2
            where
              -- If there are two sequences with the same ETHID, take the one with the smaller number of n
              cs2.ethid = cs.ethid
              and (
                cs2.number_n < cs.number_n  -- Choose the sequence that has less n
                or (cs2.number_n = cs.number_n and cs2.sample_name < cs.sample_name)  -- the number of n is equal, just choose one based on name
              )
          )
        and cs.fail_reason = 'no fail reason'
      ) cs
      join viollier_test vt on cs.ethid = vt.ethid
      join consensus_sequence_mutation_nucleotide csmn on cs.sample_name = csmn.sample_name
      left join sequence_identifier si on cs.ethid = si.ethid
  ) m on s.sequence_name = m.sequence_name;


-- Create indices

-- create unique index on staging_spectrum_sequence_public_meta (sequence_name);
-- create index on staging_spectrum_sequence_public_meta (date);
-- create index on staging_spectrum_sequence_public_meta (region);
-- create index on staging_spectrum_sequence_public_meta (country);
-- create index on staging_spectrum_sequence_public_meta (division);
-- create index on staging_spectrum_sequence_public_meta (location);
-- create index on staging_spectrum_sequence_public_meta (zip_code);
-- create index on staging_spectrum_sequence_public_meta (host);
-- create index on staging_spectrum_sequence_public_meta (age);
-- create index on staging_spectrum_sequence_public_meta (sex);
-- create index on staging_spectrum_sequence_public_meta (pangolin_lineage);
create unique index on staging_spectrum_sequence_public_mutation_aa(sequence_name, aa_mutation);
create index on staging_spectrum_sequence_public_mutation_aa(sequence_name);
create index on staging_spectrum_sequence_public_mutation_aa(aa_mutation);
create index on staging_spectrum_sequence_public_mutation_aa(aa_mutation_gene, aa_mutation_position);
create index on staging_spectrum_sequence_public_mutation_aa(aa_mutation_gene, aa_mutation_position, aa_mutation_base);
create unique index on staging_spectrum_sequence_public_mutation_nucleotide(sequence_name, position, mutation_base);
create index on staging_spectrum_sequence_public_mutation_nucleotide(sequence_name);
create index on staging_spectrum_sequence_public_mutation_nucleotide(position);
create index on staging_spectrum_sequence_public_mutation_nucleotide(position, mutation_base);

-- ####################################
-- Now the same for the private version
-- ####################################

create materialized view staging_spectrum_sequence_private_meta as
select
  coalesce(si.gisaid_id, 'UNRELEASED_ETHZ_' || cs.ethid) as sequence_name,
  vt.order_date as date,
  'Europe' as region,
  'Switzerland' as country,
  coalesce(sc.english, 'Switzerland') as division,
  vt.city as location,
  vt.zip_code as zip_code,
  'Human' as host,
  bm.altersjahr as age,
  (case when bm.sex = 'Männlich' then 'Male' when bm.sex = 'Weiblich' then 'Female' end) as sex,
  'Department of Biosystems Science and Engineering, ETH Zürich' as submitting_lab,
  'Viollier AG' as originating_lab,
  coalesce(hospitalisation_type = 'HOSPITALIZED', false) as hospitalized,
  coalesce(pttod, false) as deceased,
  (case when nd.pangolin_lineage <> 'None' then nd.pangolin_lineage end) as pangolin_lineage,
  bm.impfstatus as vaccination_status,
  bm.dosen_anzahl as vaccination_doses,
  bm.impfdatum_dose1 as vaccination_first_dose_date,
  bm.impfdatum_dose2 as vaccination_second_dose_date
from
  (
    -- We take all sequences that are on GISAID
    select *
    from consensus_sequence cs
    where
      exists (
        select
        from sequence_identifier si
        where
          cs.sample_name = si.sample_name
          and si.gisaid_id is not null
      )
    union all
    select *
    from consensus_sequence cs
    where
      not exists(
        select
        from sequence_identifier si
        where
          (cs.sample_name = si.sample_name or cs.ethid = si.ethid)
          and si.gisaid_id is not null
      )
      and not exists(
        select *
        from consensus_sequence cs2
        where
          -- If there are two sequences with the same ETHID, take the one with the smaller number of n
          cs2.ethid = cs.ethid
          and (
            cs2.number_n < cs.number_n  -- Choose the sequence that has less n
            or (cs2.number_n = cs.number_n and cs2.sample_name < cs.sample_name)  -- the number of n is equal, just choose one based on name
          )
      )
    and cs.fail_reason = 'no fail reason'
  ) cs
  join viollier_test vt on cs.ethid = vt.ethid
  left join sequence_identifier si on vt.ethid = si.ethid
  left join swiss_canton sc on vt.canton = sc.canton_code
  left join bag_meldeformular bm on vt.sample_number = bm.sample_number
  left join consensus_sequence_nextclade_data nd on cs.sample_name = nd.sample_name
union all
select
  gs.gisaid_epi_isl as sequence_name,
  gs.date,
  sc.region,
  sc.name as country,
  gs.division,
  gs.location,
  null as zip_code,
  gs.host,
  gs.age,
  (case when gs.sex = 'Male' or gs.sex = 'Female' then gs.sex end) as sex,
  gs.submitting_lab,
  gs.originating_lab,
  null as hospitalized,
  null as deceased,
  (case when gs.pangolin_lineage <> 'None' then gs.pangolin_lineage end) as pangolin_lineage,
  null as vaccination_status,
  null as vaccination_doses,
  null as vaccination_first_dose_date,
  null as vaccination_second_dose_date
from
  gisaid_api_sequence gs
  join spectrum_country sc on gs.country = sc.iso_code
where
  -- Team-W will be included because it's not in viollier_test
  (gs.strain not like '%-ETHZ-%' or lower(gs.originating_lab) = lower('Labor Team W Ag'))
  and gs.host = 'Human'
  and gs.date >= '2020-01-01';


-- Create indices

create unique index on staging_spectrum_sequence_private_meta (sequence_name);
create index on staging_spectrum_sequence_private_meta (date);
create index on staging_spectrum_sequence_private_meta (region);
create index on staging_spectrum_sequence_private_meta (country);
create index on staging_spectrum_sequence_private_meta (division);
create index on staging_spectrum_sequence_private_meta (location);
create index on staging_spectrum_sequence_private_meta (zip_code);
create index on staging_spectrum_sequence_private_meta (host);
create index on staging_spectrum_sequence_private_meta (age);
create index on staging_spectrum_sequence_private_meta (sex);
create index on staging_spectrum_sequence_private_meta (pangolin_lineage);


-- Sequencing intensity
create materialized view staging_spectrum_sequence_intensity as
with date_and_countries as (
  select
    sc.region,
    sc.name as country,
    i::date as date
  from
    generate_series('2020-01-01'::date, current_date, '1 day'::interval) i,
    spectrum_country sc
),
sequenced as (
  select
    ss.country,
    ss.date,
    count(*) as sequenced,
    sum(case when ss.submitting_lab in (
      'Department of Biosystems Science and Engineering, ETH Zürich',
      'HUG, Laboratory of Virology and the Health2030 Genome Center'
    ) then 1 else 0 end) as sequenced_surveillance
  from staging_spectrum_sequence_public_meta ss
  group by ss.country, ss.date
),
cases as (
  select
    sc.name as country,
    gc.date,
    new_cases as cases
  from
    ext_owid_global_cases gc
    join spectrum_country sc on gc.iso_country = sc.iso_code
)
select
  dc.date,
  dc.region,
  dc.country,
  sum(coalesce(c.cases, 0)) as cases,
  sum(coalesce(s.sequenced, 0)) as sequenced,
  sum(coalesce(s.sequenced_surveillance, 0)) as sequenced_surveillance
from
  date_and_countries dc
  left join sequenced s on dc.date = s.date and dc.country = s.country
  left join cases c on dc.date = c.date and dc.country = c.country
group by rollup (dc.date, dc.region, dc.country)
order by dc.date;

create unique index on staging_spectrum_sequence_intensity (date, region, country);
create index on staging_spectrum_sequence_intensity (region);
create index on staging_spectrum_sequence_intensity (country);


-- Amino acid mutations of pangolin lineages per country and date
create materialized view staging_spectrum_pangolin_lineage_mutation as
select
  s.region,
  s.country,
  s.date,
  s.pangolin_lineage,
  m.aa_mutation,
  count(*) as count
from
  staging_spectrum_sequence_public_meta s
  join staging_spectrum_sequence_public_mutation_aa m on s.sequence_name = m.sequence_name
group by s.region, s.country, s.date, s.pangolin_lineage, m.aa_mutation;

create unique index on staging_spectrum_pangolin_lineage_mutation(region, country, date, pangolin_lineage, aa_mutation);
create index on staging_spectrum_pangolin_lineage_mutation (date, pangolin_lineage);
create index on staging_spectrum_pangolin_lineage_mutation (region, date, pangolin_lineage);
create index on staging_spectrum_pangolin_lineage_mutation (country, date, pangolin_lineage);


-- Nucleotide mutations of pangolin lineages per country and date
create materialized view staging_spectrum_pangolin_lineage_mutation_nucleotide as
select
  s.region,
  s.country,
  s.date,
  s.pangolin_lineage,
  m.position || m.mutation_base as nuc_mutation,
  count(*) as count
from
  staging_spectrum_sequence_public_meta s
  join staging_spectrum_sequence_public_mutation_nucleotide m on s.sequence_name = m.sequence_name
group by s.region, s.country, s.date, s.pangolin_lineage, m.position, m.mutation_base;

create unique index on staging_spectrum_pangolin_lineage_mutation_nucleotide(region, country, date, pangolin_lineage, nuc_mutation);
create index on staging_spectrum_pangolin_lineage_mutation_nucleotide (date, pangolin_lineage);
create index on staging_spectrum_pangolin_lineage_mutation_nucleotide (region, date, pangolin_lineage);
create index on staging_spectrum_pangolin_lineage_mutation_nucleotide (country, date, pangolin_lineage);


-- Additional confirmed case information for Switzerland
create materialized view staging_spectrum_swiss_cases as
select
  m.fall_dt as date,
  sc.english as division,
  m.altersjahr as age,
  (case when m.sex = 'Männlich' then 'Male' when m.sex = 'Weiblich' then 'Female' end) as sex,
  coalesce(m.hospitalisation = 1, false) as hospitalized,
  coalesce(m.pttod, false) as deceased,
  count(*) as count
from
  bag_dashboard_meldeformular m
  left join swiss_canton sc on m.ktn = sc.canton_code
group by m.fall_dt, sc.english, m.altersjahr, m.sex, hospitalized, deceased
order by m.fall_dt, sc.english, m.altersjahr, m.sex, hospitalized, deceased;

create unique index on staging_spectrum_swiss_cases (date, division, age, sex, hospitalized, deceased);
create index on staging_spectrum_swiss_cases (date);


grant select
on table
  staging_spectrum_sequence_public_meta,
  staging_spectrum_sequence_public_mutation_aa,
  staging_spectrum_sequence_public_mutation_nucleotide,
  staging_spectrum_sequence_private_meta,
  staging_spectrum_sequence_intensity,
  staging_spectrum_pangolin_lineage_mutation,
  staging_spectrum_pangolin_lineage_mutation_nucleotide,
  staging_spectrum_swiss_cases
to spectrum;

alter materialized view staging_spectrum_sequence_public_meta
rename to spectrum_sequence_public_meta;
alter materialized view staging_spectrum_sequence_public_mutation_aa
rename to spectrum_sequence_public_mutation_aa;
alter materialized view staging_spectrum_sequence_public_mutation_nucleotide
rename to spectrum_sequence_public_mutation_nucleotide;
alter materialized view staging_spectrum_sequence_private_meta
rename to spectrum_sequence_private_meta;
alter materialized view staging_spectrum_sequence_intensity
rename to spectrum_sequence_intensity;
alter materialized view staging_spectrum_pangolin_lineage_mutation
rename to spectrum_pangolin_lineage_mutation;
alter materialized view staging_spectrum_pangolin_lineage_mutation_nucleotide
rename to spectrum_pangolin_lineage_mutation_nucleotide;
alter materialized view staging_spectrum_swiss_cases
rename to spectrum_swiss_cases;
