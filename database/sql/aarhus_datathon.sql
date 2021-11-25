select host, count(*), min(date), max(date)
from gisaid_api_sequence gs
where
  country_original = 'Denmark'
group by host;

with moderate_mutations as (
  select distinct position, variant_base
  from tmp_aarhus_mutations
  where impact = 'MODERATE'
)
select mutation, count(*)
from
  (
    select
      m.gisaid_epi_isl, m.position || m.mutation as mutation
    from
      (
        select *
        from gisaid_api_sequence gs
        where
          gs.country_original in ('Switzerland', 'Denmark')
          and (pangolin_lineage = 'B.1.617.2' or pangolin_lineage like 'AY%')
      ) gs
      join gisaid_api_sequence_mutation_nucleotide m on gs.gisaid_epi_isl = m.gisaid_epi_isl
      join moderate_mutations tam
        on m.position = tam.position and m.mutation = tam.variant_base
  ) as m
group by mutation
having count(*) > 50
order by count(*) desc;


select
  substr(gisaid_epi_isl, 9) as "id",
  null as"Virus name",
  null as"Type",
  null as"Accession ID",
  date as"Collection date",
  null as"Location",
  null as"region",
  null as"country",
  null as"division",
  null as"Additional location information",
  null as"Sequence length",
  host as"Host",
  null as"Patient age",
  null as"Gender",
  null as"Clade",
  pangolin_lineage as"pangolin_lineage",
  null as"Pangolin version",
  null as"Variant",
  null as"AA Substitutions",
  null as"date_submitted",
  null as"Is reference?",
  null as"Is complete?",
  null as"Is high coverage?",
  null as"Is low coverage?",
  null as"N-Content",
  null as"GC-Content",
  null as"species",
  null as"date",
  null as"Patient status",
  null as"Passage",
  null as"Specimen",
  null as"Additional host information",
  null as"Lineage",
  null as"Sampling strategy",
  null as"Last vaccinated",
  null as"count_N",
  null as"count_S"
from gisaid_api_sequence gs
where
  gs.country_original = 'South Africa';

select
  substr(m.gisaid_epi_isl, 9) as "ID",
  string_agg(m.aa_mutation, ';') as mut
from
  gisaid_api_sequence_nextclade_mutation_aa m
  join gisaid_api_sequence gs on m.gisaid_epi_isl = gs.gisaid_epi_isl
where
  gs.country_original = 'South Africa'
group by m.gisaid_epi_isl;
