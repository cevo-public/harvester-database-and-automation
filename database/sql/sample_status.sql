-- A collection of queries to check the status of some samples.

select
    *
from
    bag_sequence_report
where auftraggeber_nummer in (<missing>);

-- Did the samples fail?
select
    sample_name,
    fail_reason
from consensus_sequence cs
left join viollier_test vt on cs.ethid = vt.ethid
where vt.sample_number in (<missing>);

-- Did we submit them?
select
    *
from
     sequence_identifier
where
      ethid in (<missing>);

-- Are they on GISAID?
select
    strain,
    date_submitted,
    gisaid_epi_isl
from gisaid_api_sequence
where submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zürich'
and strain like '<missing>' or strain like '<missing>';

-- Are the other samples submitted on GISAID?
select
    strain,
    date_submitted,
    gisaid_epi_isl
from gisaid_api_sequence
where submitting_lab = 'Department of Biosystems Science and Engineering, ETH Zürich'
and strain in (<missing>);

-- When was a sample sent for sequencing and to where? Is it done yet?
select
    vt.sample_number,
    vp.viollier_plate_name,
    left_viollier_date,
    vp.sequencing_center,
    number_n,
    si.spsp_uploaded_at,
    si.gisaid_id
from viollier_plate vp
left join viollier_test__viollier_plate vtvp on vp.viollier_plate_name = vtvp.viollier_plate_name
left join viollier_test vt on vtvp.sample_number = vt.sample_number
left join consensus_sequence cs on vt.ethid = cs.ethid
left join sequence_identifier si on vt.ethid = si.ethid
where vt.sample_number = <missing>;

-- Can I get some ethids from a plate, to see if the sequences are available?
select
    vtvp.viollier_plate_name,
    vtvp.sample_number,
    vt.ethid
from
    viollier_test__viollier_plate vtvp
    left join viollier_test vt on vtvp.sample_number = vt.sample_number
where viollier_plate_name = '<missing>';

-- Are all the samples from the plate alread in the database under a particular batch?
select
    vtvp.viollier_plate_name,
    vtvp.sample_number,
    vt.ethid,
    cs.sequencing_batch,
    left_viollier_date
from
    viollier_test__viollier_plate vtvp
    left join viollier_test vt on vtvp.sample_number = vt.sample_number
    left join viollier_plate vp on vp.viollier_plate_name = vtvp.viollier_plate_name
    left join consensus_sequence cs on vt.ethid = cs.ethid
where vtvp.viollier_plate_name = '<missing>';


-- Has a batch already been submitted to SPSP?
select
    x.*,
    finalized_status
from (
    select
        sequencing_batch,
        sum(case when fail_reason = 'no fail reason' then 1 else 0 end) as n_successful_seq,
        sum(case when spsp_uploaded_at is not null then 1 else 0 end) as n_spsp_uploaded
    from
        consensus_sequence cs
        left join sequence_identifier si on si.sample_name = cs.sample_name
    where cs.sequencing_batch = '<missing>'
    and cs.ethid is not null
    group by sequencing_batch) x
left join sequencing_batch_status st on st.sequencing_batch = x.sequencing_batch;

-- Why weren't some sequences submitted?
-- Were samples with same ethid already submitted (ie these are duplicates?)
select
    cs.sample_name,
    si.*
from
    consensus_sequence cs
    left join sequence_identifier si on cs.ethid = si.ethid
where cs.sequencing_batch = '<missing>'
and fail_reason = 'no fail reason'
and not exists(
    select
    from sequence_identifier si
    where si.sample_name = cs.sample_name
);

-- What plates does a batch contain?
select
    viollier_plate_name,
    count(*) as n_seqs
from consensus_sequence cs
left join viollier_test vt on cs.ethid = vt.ethid
left join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
where sequencing_batch = '<missing>'
group by viollier_plate_name;
