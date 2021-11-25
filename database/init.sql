-- Sequencing batch status
create table sequencing_batch_status (
  sequencing_batch text primary key,
  finalized_status boolean
);

-- Country coordinates
create table ext_country_coordinates (
  iso_code text not null,
  latitude float,
  longitude float
);

-- FOPH travel quarantine list
create table foph_travel_quarantine (
  country text,
  iso_code text not null,
  region text,
  date_effective date not null
);

-- Frameshift deletion diagnostics output by V-pipe
create table frameshift_deletion_diagnostic (
  sample_name text not null
    references consensus_sequence on delete cascade,
  start_position int not null,
  indel_type text,
  length int not null,
  gene_region text,
  reads_all int,
  reads_fwd int,
  reads_rev int,
  deletions int,
  freq_del float,
  freq_del_fwd float,
  freq_del_rev float,
  deletions_fwd int,
  deletions_rev int,
  insertions int,
  freq_insert float,
  freq_insert_fwd float,
  freq_insert_rev float,
  insertions_fwd int,
  insertions_rev int,
  matches_ref int,
  pos_critical_inserts text,
  pos_critical_dels text,
  homopolymeric boolean,
  ref_base text,
  indel_diagnosis text,
  indel_position text,
  primary key (sample_name, start_position, indel_type)
);

create type date_type as enum (
  'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'
);

-- FSO tourist accomodation statistics
create table ext_fso_tourist_accommodation (
  iso_country text not null,
  country text,
  date date not null,
  date_type date_type not null,
  n_arrivals int,
  primary key (iso_country, date)
);

-- FSO cross-border commuter statistics
create table ext_fso_cross_border_commuters (
  iso_country text not null,
  country text,
  date date not null,
  date_type date_type not null,
  wirtschaftsabteilung text not null,
  n_permits float,
  primary key (iso_country, date, wirtschaftsabteilung)
);


-- Demography: age structure
create table ext_demography_age
(
  iso_country text not null
    references country (iso_code),
  age_group text not null,
  count integer not null,
  primary key (iso_country, age_group)
);

create index on ext_demography_age (iso_country);

-- OWID global case counts
create table ext_owid_global_cases (
  iso_country text not null
    references country (iso_code),
  country text,
  date date not null,
  new_cases_per_million float,
  new_deaths_per_million float,
  new_cases int,
  new_deaths int,
  primary key (iso_country, date)
);

-- Canton codes and names
create table swiss_canton (
  canton_code text primary key,
  german text,
  french text,
  italian text,
  english text
);


-- GISAID

create table gisaid_sequence (
  strain text primary key,
  virus text,
  gisaid_epi_isl text,
  genbank_accession text,
  date date,
  date_str text,
  region text,
  country text,
  division text,
  location text,
  region_exposure text,
  country_exposure text,
  division_exposure text,
  segment text,
  length int,
  host text,
  age int,
  sex text,
  nextstrain_clade text,
  pangolin_lineage text,
  gisaid_clade text,
  originating_lab text,
  submitting_lab text,
  authors text,
  url text,
  title text,
  paper_url text,
  date_submitted date,
  purpose_of_sequencing text,
  original_seq text,
  aligned_seq text,
  iso_country text
    references country (iso_code),
  iso_country_exposure text
    references country (iso_code),

  nextclade_clade text,
  nextclade_qc_overall_score float,
  nextclade_qc_overall_status text,
  nextclade_total_gaps integer,
  nextclade_total_insertions integer,
  nextclade_total_missing integer,
  nextclade_total_mutations integer,
  nextclade_total_non_acgtns integer,
  nextclade_total_pcr_primer_changes integer,
  nextclade_alignment_start integer,
  nextclade_alignment_end integer,
  nextclade_alignment_score integer,
  nextclade_qc_missing_data_score float,
  nextclade_qc_missing_data_status text,
  nextclade_qc_missing_data_total integer,
  nextclade_qc_mixed_sites_score float,
  nextclade_qc_mixed_sites_status text,
  nextclade_qc_mixed_sites_total integer,
  nextclade_qc_private_mutations_cutoff integer,
  nextclade_qc_private_mutations_excess integer,
  nextclade_qc_private_mutations_score float,
  nextclade_qc_private_mutations_status text,
  nextclade_qc_private_mutations_total integer,
  nextclade_qc_snp_clusters_clustered text,
  nextclade_qc_snp_clusters_score float,
  nextclade_qc_snp_clusters_status text,
  nextclade_qc_snp_clusters_total integer,
  nextclade_errors text
);

create index gisaid_sequence_age_index
	on gisaid_sequence (age);

create index gisaid_sequence_country_exposure_index
	on gisaid_sequence (country_exposure);

create index gisaid_sequence_country_index
	on gisaid_sequence (country);

create index gisaid_sequence_date_index
	on gisaid_sequence (date);

create index gisaid_sequence_date_submitted_index
	on gisaid_sequence (date_submitted);

create index gisaid_sequence_division_exposure_index
	on gisaid_sequence (division_exposure);

create index gisaid_sequence_division_index
	on gisaid_sequence (division);

create index gisaid_sequence_gisaid_epi_isl_index
	on gisaid_sequence (gisaid_epi_isl);

create index gisaid_sequence_host_index
	on gisaid_sequence (host);

create index gisaid_sequence_originating_lab_index
	on gisaid_sequence (originating_lab);

create index gisaid_sequence_region_exposure_index
	on gisaid_sequence (region_exposure);

create index gisaid_sequence_region_index
	on gisaid_sequence (region);

create index gisaid_sequence_sex_index
	on gisaid_sequence (sex);

create index gisaid_sequence_submitting_lab_index
	on gisaid_sequence (submitting_lab);

create index on gisaid_sequence (iso_country);


create table gisaid_sequence_mutation_nucleotide
(
	strain text not null,
	position int not null,
	mutation text not null,
	primary key (strain, position, mutation)
);

create index gisaid_sequence_mutation_nucleotide_sample_name_index
	on gisaid_sequence_mutation_nucleotide (strain);

create index gisaid_sequence_mutation_nucleotide_position_index
	on gisaid_sequence_mutation_nucleotide (position);


create table gisaid_sequence_nextclade_mutation_aa
(
  strain text not null
    references gisaid_sequence on update cascade on delete cascade,
  aa_mutation text not null,
  primary key (strain, aa_mutation)
);

create index gisaid_sequence_nextclade_mutation_aa_strain_index
	on gisaid_sequence_nextclade_mutation_aa (strain);

create index gisaid_sequence_nextclade_mutation_aa_aa_mutation_index
	on gisaid_sequence_nextclade_mutation_aa (aa_mutation);


create table gisaid_sequence_close_country (
  id serial primary key ,
  strain text,
  close_country text not null,
  close_strain text
);

create index on gisaid_sequence_close_country (strain);
create index on gisaid_sequence_close_country (close_country);
create index on gisaid_sequence_close_country (close_strain);


-- GISAID API Feed
create table gisaid_api_sequence (
  updated_at timestamp not null,
  gisaid_epi_isl text primary key,
  strain text unique,
  virus text,
  date date,
  date_original text,
  country text references country (iso_code),
  region_original text,
  country_original text,
  division text,
  location text,
  host text,
  age int,
  sex text,
  pangolin_lineage text,
  gisaid_clade text,
  originating_lab text,
  submitting_lab text,
  authors text,
  date_submitted date,
  sampling_strategy text,
  seq_original text,
  seq_aligned text,

  nextclade_clade text,
  nextclade_qc_overall_score float,
  nextclade_qc_overall_status text,
  nextclade_total_gaps integer,
  nextclade_total_insertions integer,
  nextclade_total_missing integer,
  nextclade_total_mutations integer,
  nextclade_total_non_acgtns integer,
  nextclade_total_pcr_primer_changes integer,
  nextclade_alignment_start integer,
  nextclade_alignment_end integer,
  nextclade_alignment_score integer,
  nextclade_qc_missing_data_score float,
  nextclade_qc_missing_data_status text,
  nextclade_qc_missing_data_total integer,
  nextclade_qc_mixed_sites_score float,
  nextclade_qc_mixed_sites_status text,
  nextclade_qc_mixed_sites_total integer,
  nextclade_qc_private_mutations_cutoff integer,
  nextclade_qc_private_mutations_excess integer,
  nextclade_qc_private_mutations_score float,
  nextclade_qc_private_mutations_status text,
  nextclade_qc_private_mutations_total integer,
  nextclade_qc_snp_clusters_clustered text,
  nextclade_qc_snp_clusters_score float,
  nextclade_qc_snp_clusters_status text,
  nextclade_qc_snp_clusters_total integer,
  nextclade_errors text
);

create table gisaid_api_sequence_nextclade_mutation_aa
(
  gisaid_epi_isl text not null
    references gisaid_api_sequence (gisaid_epi_isl) on update cascade on delete cascade,
  aa_mutation text not null,
  primary key (gisaid_epi_isl, aa_mutation)
);


create table gisaid_api_sequence_mutation_nucleotide
(
  gisaid_epi_isl text not null
    references gisaid_api_sequence (gisaid_epi_isl) on update cascade on delete cascade,
  position int not null,
  mutation text not null,
  primary key (gisaid_epi_isl, position, mutation)
);

create index on gisaid_api_sequence_mutation_nucleotide (gisaid_epi_isl);
create index on gisaid_api_sequence_mutation_nucleotide (position);
create index on gisaid_api_sequence_mutation_nucleotide (position, mutation);


-- Released sequence IDs
create table sequence_identifier (
  ethid integer primary key,
  gisaid_id text unique,
  sample_name text unique,
  gisaid_uploaded_at date,
  ena_id text unique
);


-- Sequences
create table consensus_sequence (
  sample_name text primary key,
  ethid integer,
  header text,
  seq text,
  coverage real,
  r1_basequal text,
  r2_basequal text,
  rejreads real,
  alnreads real,
  insertsize integer,
  consensus_n integer,
  consensus_lcbases integer,
  consensus_diffbases integer,
  snvs integer,
  snvs_majority integer,
  divergence integer,
  excess_divergence real,
  number_n integer,
  number_gaps integer,
  clusters text,
  gaps text,
  all_snps text,
  flagging_reason text,
  variant_of_concern text,
  is_random boolean,
  dont_release boolean
);


create table consensus_sequence_mutation_nucleotide
(
	sample_name text not null
	  references consensus_sequence on update cascade on delete cascade,
	position int not null,
	mutation text not null,
	primary key (sample_name, position, mutation)
);

create index consensus_sequence_mutation_nucleotide_sample_name_index
	on consensus_sequence_mutation_nucleotide (sample_name);

create index consensus_sequence_mutation_nucleotide_position_index
	on consensus_sequence_mutation_nucleotide (position);


create table consensus_sequence_nextclade_data
(
  sample_name text primary key,
  nextclade_clade text,
  nextclade_qc_overall_score float,
  nextclade_qc_overall_status text,
  nextclade_total_gaps integer,
  nextclade_total_insertions integer,
  nextclade_total_missing integer,
  nextclade_total_mutations integer,
  nextclade_total_non_acgtns integer,
  nextclade_total_pcr_primer_changes integer,
  nextclade_alignment_start integer,
  nextclade_alignment_end integer,
  nextclade_alignment_score integer,
  nextclade_qc_missing_data_score float,
  nextclade_qc_missing_data_status text,
  nextclade_qc_missing_data_total integer,
  nextclade_qc_mixed_sites_score float,
  nextclade_qc_mixed_sites_status text,
  nextclade_qc_mixed_sites_total integer,
  nextclade_qc_private_mutations_cutoff integer,
  nextclade_qc_private_mutations_excess integer,
  nextclade_qc_private_mutations_score float,
  nextclade_qc_private_mutations_status text,
  nextclade_qc_private_mutations_total integer,
  nextclade_qc_snp_clusters_clustered text,
  nextclade_qc_snp_clusters_score float,
  nextclade_qc_snp_clusters_status text,
  nextclade_qc_snp_clusters_total integer,
  nextclade_errors text,
  pangolin_lineage text,
  pangolin_probability float,
  pangolin_learn_version text,
  pangolin_status text,
  pangolin_note text
);

create index on consensus_sequence_nextclade_data (nextclade_clade);
create index on consensus_sequence_nextclade_data (pangolin_lineage);
create index on consensus_sequence_nextclade_data (pangolin_status);


create table consensus_sequence_nextclade_mutation_aa
(
  sample_name text not null
    references consensus_sequence on update cascade on delete cascade,
  aa_mutation text not null,
  primary key (sample_name, aa_mutation)
);

create index consensus_sequence_nextclade_mutation_aa_sample_name_index
	on consensus_sequence_nextclade_mutation_aa (sample_name);

create index consensus_sequence_nextclade_mutation_aa_aa_mutation_index
	on consensus_sequence_nextclade_mutation_aa (aa_mutation);


create table variant_mutation_nucleotide
(
	variant_name text not null,
	nucleotide_mutation text not null,
	corresponding_aa_mutation text,
	primary key (variant_name, nucleotide_mutation)
);


create table variant_mutation_aa
(
	variant_name text not null,
	aa_mutation text not null,
	primary key (variant_name, aa_mutation)
);


-- Swiss postal codes
create table swiss_postleitzahl (
  plz integer primary key,
  region text,
  canton text
);

-- Viollier plates
create table viollier_plate (
  viollier_plate_name text primary key,
  gfb_number text,
  fgcz_name text,
  health2030 boolean,
  left_viollier_date date,
  has_no_extract boolean,
  comment text
);


-- Viollier PCR test information
create table viollier_test (
  sample_number integer primary key,
  ethid integer,
  order_date date,
  zip_code text,
  city text,
  canton text,
  pcr_code text,
  is_positive boolean not null,
  sequenced_by_viollier boolean not null default false,
  comment text
);

create table viollier_test__viollier_plate (
  sample_number integer not null
    references viollier_test (sample_number) on update cascade on delete restrict,
  viollier_plate_name text not null
    references viollier_plate (viollier_plate_name) on update cascade on delete restrict,
  well_position text not null,
  e_gene_ct integer,
  rdrp_gene_ct integer,
  seq_request boolean,
  primary key (sample_number, viollier_plate_name, well_position)
);


-- BAG Meldeformular
create type bag_meldeformular_hospitalisation_type as enum (
  'HOSPITALIZED', 'NOT_HOSPITALIZED', 'UNKNOWN', 'NOT_FILLED'
);

create type bag_meldeformular_exp_ort_type as enum (
  'SWITZERLAND', 'ABROAD', 'SWITZERLAND_AND_ABROAD', 'UNKNOWN', 'NOT_FILLED'
);

create type bag_meldeformular_exp_enger_kontakt_pos_fall_type as enum (
  'HAD_CLOSE_CONTACT', 'DID_NOT_HAVE_A_CLOSE_CONTACT', 'UNKNOWN', 'NOT_FILLED'
);

create type bag_meldeformular_exp_kontakt_art_type as enum (
  'FAMILY_MEMBER', 'AS_MEDICAL_STAFF', 'OTHER', 'UNKNOWN', 'SCHOOL_OR_CHILD_CARE', 'WORK', 'PRIVATE_PARTY',
  'DISCO_OR_CLUB', 'BAR_OR_RESTAURANT', 'DEMONSTRATION_OR_EVENT', 'SPONTANEOUS_CROWD_OF_PEOPLE', 'NOT_FILLED'
);

create type bag_meldeformular_lab_grund_type as enum (
  'SYMPTOMS', 'OUTBREAK_INVESTIGATION', 'OTHER', 'SWISS_COVID_APP', 'NOT_FILLED'
);

create type bag_meldeformular_quarant_vor_pos_type as enum(
  'QUARANTINE', 'NO_QUARANTINE', 'UNKNOWN', 'NOT_FILLED'
);

create type bag_meldeformular_icu_aufenthalt_type as enum(
    'ICU', 'NO_ICU', 'UNKNOWN'
);

create type bag_meldeformular_impfstatus_type as enum(
  'YES', 'NO', 'UNKNOWN'
);

create table bag_meldeformular (
  sample_number integer primary key,
  fall_id integer,
  eingang_dt date,
  present_in_viollier_dataset boolean,
  fall_dt date,
  kanton text,
  altersjahr integer,
  sex text,
  manifestation_dt date,
  hospitalisation_type bag_meldeformular_hospitalisation_type,
  hospdatin date,
  pttod boolean,
  pttoddat date,
  grunderkr_diabetes boolean,
  grunderkr_cardio boolean,
  grunderkr_hypertonie boolean,
  grunderkr_resp_chron boolean,
  grunderkr_krebs boolean,
  grunderkr_immunsup boolean,
  grunderkr_andere boolean,
  grunderkr_adipos boolean,
  grunderkr_chron_nier boolean,
  grunderkr_keine boolean,
  icu_aufenthalt boolean,
  em_hospit_icu_in_dt date,
  em_hospit_icu_out_dt date,
  expo_pers_familie boolean,
  expo_pers_gemeins boolean,
  expo_pers_gesundh boolean,
  expo_pers_passagiere boolean,
  expo_pers_andere boolean,
  exp_ort bag_meldeformular_exp_ort_type,
  exp_land text,
  exp_land_cd text,
  iso_country_exp text,
  exp_von date,
  exp_bis date,
  exp_dt date,
  exp_ausland_von date,
  exp_ausland_bis date,
  exp_wann_unbek boolean,
  exp_enger_kontakt_pos_fall bag_meldeformular_exp_enger_kontakt_pos_fall_type,
  exp_kontakt_art bag_meldeformular_quarant_vor_pos_type,
  anzahl_erg integer,
  anzahl_em integer,
  quarant_vor_pos boolean,
  lab_grund bag_meldeformular_lab_grund_type,
  lab_grund_txt text,
  form_version text,
  variant_of_concern boolean,
  variant_of_concern_typ text,
  geimpft_info_von_anamnese boolean,
  geimpft_info_von_ausweis boolean,
  geimpft_info_von_hausarzt boolean,
  impfstatus bag_meldeformular_impfstatus_type,
  dosen_anzahl integer,
  impfdatum_dose1 date,
  impfdatum_dose2 date,
  filename text,
  comment text
);


-- General data for Switzerland

-- Demographic balance by age and canton (px-x-0102020000_104)
-- Provided by the Swiss Federal Statistical Office
-- https://www.pxweb.bfs.admin.ch/pxweb/en/px-x-0102020000_104/px-x-0102020000_104/px-x-0102020000_104.px
create table ext_swiss_demographic
(
	demographic_component text not null,
	canton text not null,
	sex text not null,
	age int not null,
	year int not null,
	count int not null,
	primary key (demographic_component, canton, sex, age, year)
);

create index switzerland_demographic_demographic_component_index
	on switzerland_demographic (demographic_component);

create index switzerland_demographic_year_index
	on switzerland_demographic (year);


-- Other general data

create table country
(
  iso_code text primary key,
  iso_code_numeric int unique,
  name_english text not null unique,
  name_german text unique,
  name_french text unique,
  name_italian text unique,
  region text not null
);


-- A small collection of country information that are needed/useful.
-- The german names are those used in the BAG meldeformular and are not complete at all.
create table country_old
(
  iso3166_alpha3_code text primary key,
  german_name text,
  english_name text
);


-- Data is updated every day and is provided by the BAG through a Polybox
create table bag_test_numbers
(
    date date,
    positive_tests integer,
    negative_tests integer,
    canton text,
    age_group text
);

create index bag_test_numbers_age_group_index
    on bag_test_numbers (age_group);

create index bag_test_numbers_canton_index
    on bag_test_numbers (canton);

create index bag_test_numbers_date_index
    on bag_test_numbers (date);


-- Tailored to the dashboard, the data is updated every day and is provided by the BAG through a Polybox
create table dashboard_state
(
  last_data_update date
);

create table bag_dashboard_meldeformular
(
	eingang_dt date,
	fall_dt date,
	ktn text,
	altersjahr integer,
	sex text,
	manifestation_dt date,
	hospitalisation integer,
	hospdatin date,
	pttod boolean,
	pttoddat date,
	grunderkr_diabetes boolean,
	grunderkr_cardio boolean,
	grunderkr_hypertonie boolean,
	grunderkr_resp_chron boolean,
	grunderkr_krebs boolean,
	grunderkr_immunsup boolean,
	grunderkr_andere boolean,
	grunderkr_keine boolean,
	icu_aufenthalt integer,
	em_hospit_icu_in_dt date,
	em_hospit_icu_out_dt date,
	expo_pers_familie boolean,
	expo_pers_gemeins boolean,
	expo_pers_gesundh boolean,
	expo_pers_passagiere boolean,
	expo_pers_andere boolean,
	exp_ort integer,
	exp_land text,
	exp_land_cd text,
	exp_von date,
	exp_bis date,
	exp_dt date,
	exp_ausland_von date,
	exp_ausland_bis date,
	exp_wann_unbek integer,
	exp_enger_kontakt_pos_fall integer,
	exp_kontakt_art integer,
	anzahl_erg integer,
	anzahl_em integer,
	quarant_vor_pos integer,
	lab_grund integer,
	lab_grund_txt text,
	form_version text,
	confirmed_variant_of_concern_txt text,
	gen_variant text
);

create index bag_dashboard_meldeformular_altersjahr_index
	on bag_dashboard_meldeformular (altersjahr);

create index bag_dashboard_meldeformular_fall_dt_index
	on bag_dashboard_meldeformular (fall_dt);

create index bag_dashboard_meldeformular_hospdatin_index
	on bag_dashboard_meldeformular (hospdatin);

create index bag_dashboard_meldeformular_hospitalisation_index
	on bag_dashboard_meldeformular (hospitalisation);

create index bag_dashboard_meldeformular_ktn_index
	on bag_dashboard_meldeformular (ktn);

create index bag_dashboard_meldeformular_manifestation_dt_index
	on bag_dashboard_meldeformular (manifestation_dt);

create index bag_dashboard_meldeformular_pttod_index
	on bag_dashboard_meldeformular (pttod);

create index bag_dashboard_meldeformular_pttoddat_index
	on bag_dashboard_meldeformular (pttoddat);

create index bag_dashboard_meldeformular_sex_index
	on bag_dashboard_meldeformular (sex);

create index bag_dashboard_meldeformular_confirmed_variant_index
	on bag_dashboard_meldeformular (confirmed_variant_of_concern_txt);

create index bag_dashboard_meldeformular_gen_variant_index
	on bag_dashboard_meldeformular (gen_variant);

create view dashboard_main_view as
select
  bdm.fall_dt,
  coalesce(bdm.ktn, 'Unknown') as canton,
  (case
    when bdm.ktn in ('GE', 'VD', 'VS') then 'Lake Geneva region'
    when bdm.ktn in ('BE', 'SO', 'FR', 'NE', 'JU') then 'Espace Mittelland'
    when bdm.ktn in ('BS', 'BL', 'AG') then 'Grossregion Nordwestschweiz'
    when bdm.ktn in ('ZH') then 'Grossregion Zurich'
    when bdm.ktn in ('SG', 'TG', 'AI', 'AR', 'GL', 'SH', 'GR') then 'Ostschweiz'
    when bdm.ktn in ('UR', 'SZ', 'OW', 'NW', 'LU', 'ZG') then 'Central Switzerland'
    when bdm.ktn in ('TI') then 'Grossregion Tessin'
    when bdm.ktn in ('FL') then 'Fürstentum Liechtenstein'
    else 'Unknown'
  end) as grossregion,
  (case
    when altersjahr < 10 then '0-9'
    when altersjahr between 10 and 19 then '10-19'
    when altersjahr between 20 and 29 then '20-29'
    when altersjahr between 30 and 39 then '30-39'
    when altersjahr between 40 and 49 then '40-49'
    when altersjahr between 50 and 59 then '50-59'
    when altersjahr between 60 and 69 then '60-69'
    when altersjahr between 70 and 79 then '70-79'
    when altersjahr >= 80 then '80+'
    else 'Unknown'
  end) as age_group,
--   (case
--     when altersjahr <= 6 then '0-6'
--     when altersjahr between 7 and 12 then '07-12'
--     when altersjahr between 13 and 17 then '13-17'
--     when altersjahr between 18 and 24 then '18-24'
--     when altersjahr between 25 and 34 then '25-34'
--     when altersjahr between 35 and 44 then '35-44'
--     when altersjahr between 45 and 54 then '45-54'
--     when altersjahr between 55 and 64 then '55-64'
--     when altersjahr between 65 and 74 then '65-74'
--     when altersjahr >= 75 then '75+'
--     else 'Unknown'
--   end) as age_group,
  true as positive_test,
  1 as mult,
  bdm.hospdatin,
  bdm.pttoddat,
  bdm.em_hospit_icu_in_dt,
  bdm.hospitalisation,
  pttod,
  icu_aufenthalt,
  (case
    when sex = 'Männlich' then 'Male'
    when sex = 'Weiblich' then 'Female'
    else 'Unknown'
  end) as sex,
  (case
    when exp_ort = 1 then 'Non-travel-related'
    when exp_ort = 2 or exp_ort = 3 then 'Travel-related'
    else 'Unknown'
  end) travel_class,
  (case
    when exp_kontakt_art = 1 then 'Family member'
    when exp_kontakt_art = 2 then 'as medical staff'
    when exp_kontakt_art = 3 then 'other contacts'
    when exp_kontakt_art = 4 then 'Unknown'
    when exp_kontakt_art = 5 then 'School/child care etc'
    when exp_kontakt_art = 6 then 'Work'
    when exp_kontakt_art = 7 then 'private party'
    when exp_kontakt_art = 8 then 'Disco/Club'
    when exp_kontakt_art = 9 then 'Bar/Restaurant'
    when exp_kontakt_art = 10 then 'Demonstration/Event'
    when exp_kontakt_art = 11 then 'spontaneous crowd of people'
    when exp_kontakt_art = 12 then null
    else 'Unknown'
  end) as exp_kontakt_art,
  (case
    when quarant_vor_pos = 1 then 'Yes'
    when quarant_vor_pos = 2 then 'No'
    else 'Unknown'
  end) as quarant_vor_pos,
  (case
    when lab_grund = 1 then 'Symptoms compatible with COVID-19'
    when lab_grund = 2 then 'Outbreak investigation'
    when lab_grund = 3 then 'Other'
    when lab_grund = 4 then 'SwissCovid App'
    else 'Unknown'
  end) as lab_grund,
  exp_land,
  c.iso3166_alpha3_code as exp_land_code,
  grunderkr_diabetes,
  grunderkr_cardio,
  grunderkr_hypertonie,
  grunderkr_resp_chron,
  grunderkr_krebs,
  grunderkr_immunsup,
  grunderkr_andere,
  grunderkr_keine
from
  bag_dashboard_meldeformular bdm
  left join country_old c on bdm.exp_land = c.german_name
union all
select
  btn.date as fall_dt,
  btn.canton,
  (case
    when btn.canton in ('GE', 'VD', 'VS') then 'Lake Geneva region'
    when btn.canton in ('BE', 'SO', 'FR', 'NE', 'JU') then 'Espace Mittelland'
    when btn.canton in ('BS', 'BL', 'AG') then 'Grossregion Nordwestschweiz'
    when btn.canton in ('ZH') then 'Grossregion Zurich'
    when btn.canton in ('SG', 'TG', 'AI', 'AR', 'GL', 'SH', 'GR') then 'Ostschweiz'
    when btn.canton in ('UR', 'SZ', 'OW', 'NW', 'LU', 'ZG') then 'Central Switzerland'
    when btn.canton in ('TI') then 'Grossregion Tessin'
    when btn.canton in ('FL') then 'Fürstentum Liechtenstein'
    else 'Unknown'
  end) as grossregion,
  replace(btn.age_group, ' ', '') as age_group,
  false as positive_test,
  btn.negative_tests as mult,
  null, null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null
from bag_test_numbers btn;

create view dashboard_population_view as
select
  sd.canton,
  (case
    when sd.canton in ('GE', 'VD', 'VS') then 'Lake Geneva region'
    when sd.canton in ('BE', 'SO', 'FR', 'NE', 'JU') then 'Espace Mittelland'
    when sd.canton in ('BS', 'BL', 'AG') then 'Grossregion Nordwestschweiz'
    when sd.canton in ('ZH') then 'Grossregion Zurich'
    when sd.canton in ('SG', 'TG', 'AI', 'AR', 'GL', 'SH', 'GR') then 'Ostschweiz'
    when sd.canton in ('UR', 'SZ', 'OW', 'NW', 'LU', 'ZG') then 'Central Switzerland'
    when sd.canton in ('TI') then 'Grossregion Tessin'
    when sd.canton in ('FL') then 'Fürstentum Liechtenstein'
  end) as grossregion,
  (case
    when sd.sex = 'Männlich' then 'Male'
    when sd.sex = 'Weiblich' then 'Female'
  end) as sex,
  (case
    when age < 10 then '0-9'
    when age between 10 and 19 then '10-19'
    when age between 20 and 29 then '20-29'
    when age between 30 and 39 then '30-39'
    when age between 40 and 49 then '40-49'
    when age between 50 and 59 then '50-59'
    when age between 60 and 69 then '60-69'
    when age between 70 and 79 then '70-79'
    when age >= 80 then '80+'
  end) as age_group,
--   (case
--     when age <= 6 then '0-6'
--     when age between 7 and 12 then '07-12'
--     when age between 13 and 17 then '13-17'
--     when age between 18 and 24 then '18-24'
--     when age between 25 and 34 then '25-34'
--     when age between 35 and 44 then '35-44'
--     when age between 45 and 54 then '45-54'
--     when age between 55 and 64 then '55-64'
--     when age between 65 and 74 then '65-74'
--     when age >= 75 then '75+'
--   end) as age_group,
  sd.year,
  sd.count
from ext_swiss_demographic sd
where
  sd.demographic_component = 'Population on 31 December'
  and sd.year = 2019;


-- For the automated pipeline
create table automation_state
(
	program_name text primary key,
	state text not null
);


-- Information about pangolin lineages

create table pangolin_lineage_alias
(
  alias text primary key,
  full_name text not null unique
);


-- Problematic sites
create table ext_problematic_site
(
  position integer primary key,
  filter text not null,
  info text
);
