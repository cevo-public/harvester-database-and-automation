# Title     : Copy fastqs to droppoint that's synced with Viollier SFTP.
# Objective : Fetch quality controlled sequences from database that don't have GISAID IDs assigned yet. Generate a fasta file and metadata file for these sequences for submission to SPSP.
# Usage     : Called by the docker container spsp_transferer.

# TODO: automatically update improved sequences?
# TODO: programatically fill in V-pipe version, sequencing methods, library prep kit to database - here the latter 2 are hardcoded to known truth as of 11.05.2021

# Load requirements
source("R/utility.R")
source("R/logger.R")
require(dplyr)
require(yaml)
require(argparse)

#' Generate SPSP submission files.
#' @param args Program arguments.
main <- function(args) {
  make_outdir(args)
  db_connection <- connect_to_db(args)
  test_sampleset_dir(args)
  db_output <- get_samples_to_release(db_connection, args)
  if (length(db_output$samples) > 0) {
    metadata <- get_sample_metadata(db_connection, args, db_output$samples)
    frameshifts_tbl <- get_frameshift_diagnostics(db_connection, metadata)
    write_out_files(metadata, frameshifts_tbl, db_output$summary, db_connection, args)
  } else {
    submission_dir <- paste(args$outdir, "for_submission/viruses", Sys.Date(), sep = "/")
    system(command = paste("rm", submission_dir))
    print(log.info(
      msg = "No samples to release.",
      fcn = paste0(args$script_name, "::", "main")))
  }
}

#' Create output directory.
make_outdir <- function(args) {
  submission_dir <- paste(args$outdir, "for_submission/viruses", Sys.Date(), sep = "/")
  sent_dir <- paste(args$outdir, "for_submission/sent", sep = "/")
  if (dir.exists(submission_dir)) {
    print(log.error(
      msg = paste("Specified outdir", args$outdir, "already has a viruses directory for today. Will not overwrite."),
      fcn = paste0(args$script_name, "::", "make_outdir")))
    stop()
  } else {
    system(command = paste("mkdir -p", submission_dir))
    system(command = paste("mkdir -p", sent_dir))
    print(log.info(
      msg = "Created outdir for submission files.",
      fcn = paste0(args$script_name, "::", "make_outdir")))
  }
}

#' Connect to database.
#' @return Database connection.
connect_to_db <- function(args) {
  db_connection <- tryCatch(
    {
      print(log.info(
        msg = "Connecting to database.",
        fcn = paste0(args$script_name, "::", "connect_to_db")))
      open_database_connection(db_instance = "server", config_file = "config.yml")
    },
    error = function(cond) {
      json_error <- log.error(
        msg = "Cannot connect to database.",
        fcn = paste0(args$script_name, "::", "connect_to_db"))
      print(json_error)
      stop()
    }
  )
  return(db_connection)
}

#' Test connection to sampleset_dir where batch files should be.
test_sampleset_dir <- function(args) {
  if (!(dir.exists(args$samplesetdir))) {
    print(log.error(
      msg = paste("Specified samplesetdir", args$samplesetdir, "does not exist."),
      fcn = paste0(args$script_name, "::", "test_sampleset_dir")))
    stop()
  } else {
    print(log.info(
      msg = "Checked that samplesetdir exists.",
      fcn = paste0(args$script_name, "::", "test_sampleset_dir")))
  }
}

#' Get samples to release to SPSP by querying the database.
#' @return List, where first entry is vector of sample names to release and
#' second entry is dataframe summarizing why other sequences in database not released.
get_samples_to_release <- function(db_connection, args) {
  print(log.info(
    msg = "Querying database for samples to release.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))
  finalized_batches <- unname(unlist(dplyr::tbl(db_connection, "sequencing_batch_status") %>%
    filter(finalized_status) %>%
    select(sequencing_batch) %>%
    collect()))
  all_db_seqs <- dplyr::tbl(db_connection, "z_consensus_sequence") %>%
    select(
      sample_name,
      ethid,
      sequencing_center,
      sequencing_batch,
      #consensus_n, ###now in consensus_sequence_meta
      #number_n, ###now in consensus_sequence_meta as diagnostic_number_m
      #fail_reason, ### now in consensus_sequence_meta as qc_result
      #dont_release, ### now in consenseus_sequence_notes as release_decision
      sequencing_plate,
      sequencing_plate_well) %>%
    collect()
  ###UNTESTED as z_consensus_sequence_meta is still unpopulated Jan 5th 2022)
  all_db_seqs_additional <- dplyr::tbl(db_connection, "z_consensus_sequence_meta") %>%
    select(
      sample_name,
      consensus_n,
      diagnostic_number_n,
      qc_result) %>%
    collect()
  all_db_seqs <- left_join(x = all_db_seqs, y = all_db_seqs_additional, by = "sample_name")
  all_db_seqs_notes <- dplyr::tbl(db_connection, "z_consensus_sequence_notes") %>%
    select(sample_name, release_decision) %>%
    collect()
  all_db_seqs <- left_join(x = all_db_seqs, y = all_db_seqs_notes, by = "sample_name")
  all_plates_mapping <- dplyr::tbl(db_connection, "z_test_plate_mapping") %>%
    select(test_id, sequencing_plate, sequencing_plate_well) %>%
    collect()
  all_db_seqs <- left_join(x = all_db_seqs, y = all_plates_mapping, by = c("sequencing_plate", "sequencing_plate_well"))

  print(log.info(
    msg = "Checking if batches are fully loaded into database.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))
  batch_completeness <- check_all_seqs_imported(
    batches = unique(all_db_seqs$sequencing_batch)[!is.na(unique(all_db_seqs$sequencing_batch))],
    samples_in_database = all_db_seqs$sample_name,
    sampleset_dir = args$samplesetdir,
    verbose = F)
  incomplete_batches <- names(batch_completeness)[!(batch_completeness)]

  print(log.info(
    msg = "Checking if sequence in database consensus_n matches sequence on D-BSSE server number_n.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))
  seq_discrepencies <- all_db_seqs %>%
    filter(diagnostic_number_n != consensus_n)

  print(log.info(
    msg = "Checking if sequences previously submitted or released.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))
  released <- dplyr::tbl(db_connection, "sequence_identifier") %>%
    filter(!is.na(gisaid_uploaded_at) | !is.na(gisaid_id) | !is.na(spsp_uploaded_at)) %>%
    collect()
  
  print(log.info(
    msg = "Checking if sequences have associated metadata in test_metadata.", #viollier_test or non_viollier_test.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))
  #has_vt_metadata <- unlist(
  #  dplyr::tbl(db_connection, "viollier_test") %>% select(ethid) %>% collect())
  #has_nvt_metadata <- unlist(
  #  dplyr::tbl(db_connection, "non_viollier_test") %>% select(sample_name) %>% collect())
  #has_metadata <- all_db_seqs$ethid[(all_db_seqs$ethid %in% has_vt_metadata) | 
  #  (all_db_seqs$sample_name %in% has_nvt_metadata)]
   has_test_metadata <- unlist(
     dplyr::tbl(db_connection, "z_test_metadata") %>% select(test_id) %>% collect())
   has_metadata <- all_db_seqs$test_id[(all_db_seqs$test_id %in% has_test_metadata)]

  print(log.info(
    msg = "Annotating sequences in database with reasons not to release, if any.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))

  all_db_seqs_annotated <- all_db_seqs %>%
    mutate(first_pass_no_fail = qc_result == "no fail reason") %>%
    ###UNTESTED: check if it's better to group by test_id
    group_by(ethid, first_pass_no_fail) %>%
    arrange(consensus_n, .by_group = T) %>%
    mutate(
      duplicate_idx = row_number(),
      qc_result = case_when(
        !(sequencing_batch %in% finalized_batches) ~ "sequencing batch not finalized according to table sequencing_batch_status",
        release_decision ~ "column release_decision in z_consensus_sequence is true",
        ethid %in% released$ethid ~ "ethid already released or submitted",
        !(test_id %in% has_metadata) ~ "no metadata in z_test_metadata",
        qc_result == "no fail reason" & is.na(ethid) ~ "null ethid",
        qc_result == "no fail reason" & duplicate_idx > 1 ~ "less complete duplicate",
        sample_name %in% seq_discrepencies$sample_name ~ "sequence discrepency between D-BSSE server and database",
        T ~ qc_result),
        warning_reason = case_when(
            sequencing_batch %in% incomplete_batches ~ "data from batch not completely loaded into database")) %>%
    ungroup()
  #UNTESTED: check if after mutating you can move on using the new fail_reason and warning reason
  fail_reason_summary <- summarize_fail_reasons(all_db_seqs_annotated)
  report_suspicious_batches(fail_reason_summary, args)
  report_null_ethids(all_db_seqs_annotated, args)
  
  #UNTESTED: this might need to be fail_reason
  to_release <- all_db_seqs_annotated %>% filter(qc_result == "no fail reason")
  print(log.info(
    msg = paste("Found", nrow(to_release), "sequences in database to release."),
    fcn = paste0(args$script_name, "::", "get_samples_to_release")))
  output <- list("samples" = to_release$sample_name, "summary" = fail_reason_summary)
  return(output)
}

#' Summarize fail reasons.
#' @return Dataframe of fail reasons by batch.
summarize_fail_reasons <- function(all_db_seqs_annotated) {
  fail_reason_summary <- all_db_seqs_annotated %>%
    group_by(sequencing_center, sequencing_batch, qc_result, warning_reason) %>%
    summarize(seq_count = n(), .groups = "drop") %>%
    group_by(sequencing_batch) %>%
    mutate(
      n_samples_batch = sum(seq_count),
      frac_batch = seq_count / n_samples_batch,
      batch_date = as.Date(
        gsub(sequencing_batch, pattern = "_.*", replacement = ""),
        format = "%Y%m%d"),
      batch_week = format(batch_date, "%Y-%W")) %>%
    arrange(desc(batch_week), sequencing_center, sequencing_batch, desc(frac_batch))
  return(fail_reason_summary)
}

#' Notify if any batches in the past 3 weeks are suspicious:
#' < 80% sequences are releasable/released; or
#' batch not fully loaded into database.
report_suspicious_batches <- function(fail_reason_summary, args) {
  suspicious_batches <- unlist(
    fail_reason_summary %>%
      filter(
        batch_date > Sys.Date() - 21,
        (qc_result == "ethid already released or submitted" & frac_batch < 0.8) |
          warning_reason == "data from batch not completely loaded into database" |
          qc_result == "no fail reason" & frac_batch < 0.8) %>%
      select(sequencing_batch))
  suspicious_batch_summary <- fail_reason_summary %>%
    filter(sequencing_batch %in% suspicious_batches)
  if (nrow(suspicious_batch_summary) > 1) {
    suspicious_batch_message <- paste(
      "Some batches have suspicious QC values:",
      format_dataframe_for_log(
        suspicious_batch_summary %>%
          mutate(seq_count = paste(seq_count, "samples")) %>%
          select(sequencing_center, sequencing_batch, qc_result, warning_reason, seq_count)),
      sep = "\n")
    print(notify.warn(
      msg = suspicious_batch_message,
      fcn = paste0(args$script_name, "::", "report_suspicious_batches")))
  } else {
    print(log.info(
      msg = "Checked that QC for batches in last 3 weeks looks okay.",
      fcn = paste0(args$script_name, "::", "report_suspicious_batches")))
  }
}

#' Warn when no ETHID is found (these should be controls).
report_null_ethids <- function(all_db_seqs_annotated, args) {
  null_ethid_summary <- all_db_seqs_annotated %>%
    filter(qc_result == "null ethid") %>%
    select(sequencing_center, sequencing_batch, sample_name)
  if (nrow(null_ethid_summary) > 1) {
    null_ethid_message <- paste(
      "ETHID not parsed from some sample names (tail shown). These assumed to be controls and not released:",
      format_dataframe_for_log(tail(null_ethid_summary)), sep = "\n")
    print(notify.warn(
      msg = null_ethid_message,
      fcn = paste0(args$script_name, "::", "report_null_ethids")))
  } else {
    print(log.info(
      msg = "Checked that ETHIDs successfully parsed from all sample names.",
      fcn = paste0(args$script_name, "::", "report_null_ethids")))
  }
}

#' Query database to assemble sequence metadata.
get_sample_metadata <- function(db_connection, args, samples) {
  print(log.info(
    msg = "Querying database for sample metadata.",
    fcn = paste0(args$script_name, "::", "get_sample_metadata")))
  query_seqs <- dplyr::tbl(db_connection, "z_consensus_sequence") %>%
    filter(sample_name %in% !! samples) %>%
    select(sample_name, sequencing_plate, sequencing_plate_well, sequencing_center, sequencing_batch) %>%
    collect()
  #UNTESTED: coverage is now coverage_mean in z_consensus_sequence_meta. 
  query_seqs_additional <- dplyr::tbl(db_connection, "z_consensus_sequence_meta") %>%
    select(sample_name, coverage_mean) %>%
    collect()
  query_seqs <- left_join(x = query_seqs, y = query_seqs_additional, by = "sample_name")
  ############FIXME: still need to understand where is_random has gone. At the moment I use purpose in z_consensus_sequence_notes
  query_seqs_is_random <- dplyr::tbl(db_connection, "z_consensus_sequence_notes") %>%
    select(sample_name, purpose) %>%
    collect()
  query_seqs <- left_join(x = query_seqs, y = query_seqs_is_random, by = "sample_name")
  all_plates_mapping <- dplyr::tbl(db_connection, "z_test_plate_mapping") %>%
    select(test_id, sequencing_plate, sequencing_plate_well) %>%
    collect()
  query_seqs <- left_join(x = query_seqs, y = all_plates_mapping, by = c("sequencing_plate", "sequencing_plate_well"))

  query_viollier <- dplyr::tbl(db_connection, "z_test_metadata") %>%  # join sequence data to sample metadata using test_id
    select(ethid, test_id, order_date, zip_code) %>%
    mutate(covv_orig_lab = "Viollier AG", covv_orig_lab_addr = "Hagmattstrasse 14, 4123 Allschwil", sample_number = test_id) %>%
    collect()
  ###Here need to change the sample number so that it is a gsub of test_id
    query_viollier <- query_viollier %>% mutate(sample_number = gsub(sample_number, pattern=".*/",replacement = "")) %>% collect()
  #REFACTOR
  join_viollier <- left_join(x = query_seqs, y = query_viollier, by = "test_id")
  query_bag_sequence_report <- dplyr::tbl(db_connection, "bag_sequence_report") %>%
    select(auftraggeber_nummer, alt_seq_id, viro_purpose) %>% collect()
  #REFACTOR
  join_viollier_w_reason <- left_join(  # merge in sequencing reason
    x = join_viollier,
    y = query_bag_sequence_report,
    by = c("sample_number" = "auftraggeber_nummer"))
  #REFACTOR

  query_canton_code <- dplyr::tbl(db_connection, "swiss_postleitzahl") %>%  # get canton based on zip_code in metadata since canton is sometimes missing
    select(plz, canton) %>%
    rename(zip_code = plz) %>% collect()
  join_canton <- left_join(x = join_viollier_w_reason, y = query_canton_code, by = "zip_code")
  #viollier_metadata <- join_canton %>% collect()

  seq_metadata <- join_canton

  #REFACTOR
  #THIS SECTION SHOULD BE REDUNDANT UNLESS THE CHANGE FROM SAMPLE_NUMBER STILL MAKES A DIFFERENCE
  #STILL TEST IN THE NEW DATABASE THE NEW VERSION TO BE SURE THAT THE CANTON PART DOES NOT MESS UP
  #query_non_viollier <- dplyr::tbl(db_connection, "non_viollier_test") %>%  # merge in special sample metadata
  #  filter(sample_name %in% !! samples)
  #join_canton_non_viollier <- left_join(x = query_non_viollier, y = query_canton_code, by = "zip_code")
  #non_viollier_metadata <- join_canton_non_viollier %>%  # merge in sequencing reason
  #  left_join(
  #      y = query_bag_sequence_report,
  #      by = c("sample_name" = "alt_seq_id")) %>%
  #      collect()
  #non_viollier_metadata <- non_viollier_metadata %>%
  #  mutate(
  #    canton = coalesce(`canton.y`, `canton.x`)) %>%  # prefer canton matched from zip code, otherwise take from canton column if filled
  #  select(-c(`canton.y`, `canton.x`))

  #REFACTOR
  #POTENTIALLY THIS PART IS NOT NECCESARY, IF WE CAN WORK WITHOUT SPLITTING THE TWO THINGS
  #seq_metadata <- coalesce_join(
  #  x = viollier_metadata, y = non_viollier_metadata, by = "sample_name", all = T)

  seq_metadata <- seq_metadata %>% dplyr::left_join(
    y = dplyr::tbl(db_connection, "lab_code_foph") %>% collect(),
    by = "covv_orig_lab",
    na_matches = "never") # join collecting_lab_code

  canton_fullname <- dplyr::tbl(db_connection, "swiss_canton") %>%  # get full canton names
    select(canton_code, english) %>%
    rename(canton = canton_code, canton_fullname = english) %>%
    collect()

  metadata_w_canton <- merge(
    x = seq_metadata, y = canton_fullname, by = "canton", all.x = T)

  qcd_metadata <- qc_sample_metadata(metadata = metadata_w_canton, args)
  spsp_formatted_metadata <- format_metadata_for_spsp(metadata = qcd_metadata, args)
  metadata <- check_mandatory_columns(metadata = spsp_formatted_metadata, args)
  return(metadata)
}

#' Check metadata for required information.
qc_sample_metadata <- function(metadata, args) {
  # Are any sequences missing the Canton? If so, replace with "UN" for "unknown".
  missing_canton_info <- metadata %>% filter(is.na(canton))
  if (nrow(missing_canton_info) > 0) {
    log.info(
      msg = paste(nrow(missing_canton_info), "samples are missing Canton information. Replacing with 'UN' for 'unknown'."),
      fcn = paste0(args$script_name, "::", "qc_sample_metadata"))
    metadata <- metadata %>%
      mutate(canton = tidyr::replace_na(data = canton, replace = "UN"))
  }
  #If you get this error and it's unexpected (the duplicate should be uploaded) this means we need to move from sample_number to test_id
  temp <- metadata %>%
    collect %>%
    group_by(sample_number) %>%
    filter(n() > 1)
  if (nrow(temp) > 0 & !all(is.na(temp$sample_number))) {
    notify.error(
      msg = paste(
        "Some sequences associated with same sample_number:",
        format_dataframe_for_log(temp %>% select(sample_name, ethid)), sep = "\n"),
      fcn = paste0(args$script_name, "::", "qc_sample_metadata"))
      stop()
  }
  # Do we have any duplicate sequences of the same ethid?
  temp <- metadata %>%
    collect %>%
    group_by(ethid) %>%
    filter(n() > 1)
  if (nrow(temp) > 0) {
    notify.error(
      msg = paste(
        "Some sequences associated with same ethid:",
        format_dataframe_for_log(temp %>% select(sample_name, ethid)), sep = "\n"),
      fcn = paste0(args$script_name, "::", "qc_sample_metadata"))
    stop()
  }
  return(metadata)
}

#' Format metadata for SPSP.
format_metadata_for_spsp <- function(metadata, args) {
  print(log.info(
     msg = "Formatting metadata for SPSP.",
     fcn = paste0(args$script_name, "::", "format_metadata_for_spsp")))
  seq_authors <- read_seq_authors(args)
  metadata_for_spsp <- metadata %>%
    summarise(
      orig_fasta_name = paste0(sample_name, ".fasta.gz"),
      strain_name = paste(
        "hCoV-19", "Switzerland", paste(canton, "ETHZ", ethid, sep = "-"),
        lubridate::year(order_date), sep = "/"),
      is_assembly_update = "No",
      isolation_date = order_date,
      location_general = case_when(
        is.na(canton_fullname) ~ paste("Europe", "Switzerland", sep = " / "),
        T ~ paste("Europe", "Switzerland", canton_fullname, sep = " / ")),
      isolation_source_description = "Human",
      host_sex = "Unknown",
      isolation_source_detailed = "Respiratory specimen",
      sequencing_purpose = case_when(
        viro_purpose %in% c("outbreak", "travel case", "screening", "surveillance") ~ "Screening",  # SPSP vocabulary distinguishes only between Screening, Clinical signs of infection, Re-infection, Infection after vaccination, Unknown, Other as of 27.05.21
        T ~ "Other"),
      library_preparation_kit = case_when(
        sequencing_center == "viollier" ~ "Illumina_COVIDSeq",
        sequencing_center == "gfb" ~ "NEB",
        sequencing_center == "h2030" ~ "Illumina_COVIDSeq",
        sequencing_center == "fgcz" & as.Date(
          gsub(sequencing_batch, pattern = "_.*", replacement = ""),
          format = "%Y%m%d") < as.Date("2021-04-19") ~ "NEB",
        sequencing_center == "fgcz" ~ "Nextera XT"),
      sequencing_platform = "Combination Illumina MiSeq and Illumina NovaSeq 5000/6000",
      assembly_method = "V-pipe",
      raw_dataset_coverage = round(coverage_mean, digits = 0),
      reporting_lab_name = "Department of Biosystems Science and Engineering, ETH Zürich; Mattenstrasse 26, 4058 Basel",
      reporting_lab_order_id = sample_name,
      collecting_lab_name = paste(covv_orig_lab, covv_orig_lab_addr, sep = "; "),
      collecting_lab_code = lab_code_foph,
      collecting_lab_order_id = sample_number,
      sequencing_lab_name = case_when(
        sequencing_center == "gfb" ~ "Genomic Facility Basel",
        sequencing_center %in% c("fgcz", "fcgz") ~ "Functional Genomics Center Zurich",
        sequencing_center == "h2030" ~ "H2030 Genome Center",
        sequencing_center == "viollier" ~ "Viollier AG"),
      reporting_authors = case_when(
        covv_orig_lab == "labor team w AG" ~ seq_authors$fgcz_teamw,
        sequencing_center == "gfb" ~ seq_authors$gfb,
        sequencing_center %in% c("fgcz", "fcgz") ~ seq_authors$fgcz_viollier,
        sequencing_center == "h2030" ~ seq_authors$h2030,
        sequencing_center == "viollier" ~ seq_authors$viollier),
      orig_fastq_name_forward = "to_assess")
  rownames(metadata_for_spsp) <- metadata$sample_name
  return(metadata_for_spsp)
}

#' Get author lists for each sequencing center.
read_seq_authors <- function(args) {
    print(log.info(
       msg = paste("Reading sequence authors from config file", args$config),
       fcn = paste0(args$script_name, "::", "read_seq_authors")))
  seq_authors <- read_yaml(file = args$config)
  return(seq_authors$authors)
}

# Check if missing any mandatory metadata.
check_mandatory_columns <- function(metadata, args) {
    print(log.info(
       msg = "Checking metadata has all mandatory columns filled.",
       fcn = paste0(args$script_name, "::", "get_sample_metadata")))
  mandatory_cols <- strsplit(
    x = read_yaml(file = args$config)[["mandatory_metadata_columns"]],
    split = " ")[[1]]
  mandatory_data <- metadata %>% select(all_of(mandatory_cols))
  incomplete_data_info <- mandatory_data %>%
    filter(!complete.cases(mandatory_data))

  if (nrow(incomplete_data_info) > 0) {
    print(log.warn(
      msg = paste(
        "Incomplete mandatory metadata for these samples. They will not be released:",
        format_dataframe_for_log(incomplete_data_info),
        sep = "\n"),
      fcn = paste0(args$script_name, "::", "check_mandatory_columns")))
    metadata <- metadata %>%
      filter(complete.cases(mandatory_data))
  }
  return(metadata)
}

#' Get frameshift diagnostic information.
get_frameshift_diagnostics <- function(db_connection, metadata) {
  print(log.info(
    msg = "Querying database for frameshift diagnostic information.",
    fcn = paste0(args$script_name, "::", "get_frameshift_diagnostics")))
  metadata$sample_name <- rownames(metadata)
  frameshifts_tbl <- dplyr::tbl(db_connection, "frameshift_deletion_diagnostic") %>%
    filter(sample_name %in% !! metadata$sample_name) %>%
    select(sample_name, indel_position, indel_diagnosis) %>%
    collect()
  colnames(frameshifts_tbl)[which(colnames(frameshifts_tbl)=="indel_position")] <- "indel_position_english"
  n_seqs_with_frameshifts <- length(unique(frameshifts_tbl$sample_name))

  frameshift_summary <- frameshifts_tbl %>% group_by(indel_diagnosis) %>%
    summarize(n_dels = n()) %>%
    arrange(desc(n_dels))

  # Add virus name to frameshift diagnostic table, filter to only seqs with complete metadata
  frameshifts_tbl <- merge(
    x = frameshifts_tbl,
    y = metadata[c("sample_name", "strain_name")],
    all.x = T, by = "sample_name") 
  return(frameshifts_tbl)
}

#' Write out files for submission to SPSP.
write_out_files <- function(metadata, frameshifts_tbl, batches_summary, db_connection, args) {
  print(log.info(
    msg = "Writing out files for submission.",
    fcn = paste0(args$script_name, "::", "write_out_files")))
  submission_dir <- paste(args$outdir, "for_submission/viruses", Sys.Date(), sep = "/")
  write.table(
    x = metadata,
    file = paste(submission_dir, paste0("metadata-file-", Sys.Date(), ".tsv"), sep = "/"),
    sep = "\t",
    row.names = F)

  write.table(
    x = frameshifts_tbl,
    file = paste(submission_dir, paste0("ETHZ-frameshift_report-", Sys.Date(), ".tsv"), sep = "/"),
    sep = "\t",
    quote = F,
    row.names = F)

  write.table(
    x = batches_summary,
    file = paste(args$outdir, "batches_summary.tsv", sep = "/"),
    sep = "\t",
    quote = F,
    row.names = F)

  header_mapping <- metadata$strain_name
  names(header_mapping) <- metadata$reporting_lab_order_id

  export_seqs_as_fasta(
    db_connection = db_connection,
    sample_names = metadata$reporting_lab_order_id,
    seq_outdir = submission_dir,
    overwrite = F,
    header_mapping = header_mapping,
    gzip = T)
}

# Production arguments
parser <- argparse::ArgumentParser()
parser$add_argument("--config", type = "character", help = "Path to spsp-config.yml.", default = "spsp-config.yml")
parser$add_argument("--samplesetdir", type = "character", help = "Path to V-pipe sample directories.", default = "/mnt/pangolin/sampleset")
parser$add_argument("--outdir", type = "character", help = "Path to output files for submission.", default = paste("/mnt/pangolin/consensus_data_for_release/spsp_submission", Sys.Date(), sep = "/"))
args <- parser$parse_args()
args[["script_name"]] <- "export_spsp_submission.R"

# Test arguments
# args <- list()
# args[["config"]] <- "../automation/spsp_transferer/database/spsp-config.yml"
# args[["samplesetdir"]] <- "/Volumes/covid19-pangolin/backup/sampleset"
# args[["outdir"]] <- "~/Downloads/test_outdir"
# args[["script_name"]] <- "export_spsp_submission.R"

# Run program
main(args = args)
