# Title     : Copy fastqs to droppoint that's synced with Viollier SFTP.
# Objective : Fetch quality controlled sequences from database that don't
#             have GISAID IDs assigned yet. Generate a fasta file and metadata
#             file for these sequences for submission to SPSP.
# Usage     : Called by the docker container spsp_transferer.
# Maintainer: Matteo Carrara <carrara@nexus.ethz.ch>

# TODO: automatically update improved sequences?
# TODO: programatically fill in V-pipe version, sequencing methods, library
#       prep kit to database - here the latter 2 are hardcoded to known truth
#       as of 24.01.2022

# Load requirements
source("R/utility.R")
source("R/logger.R")
source("R/trigger_upload_on_euler.R")

require(dplyr)
require(yaml)
require(argparse)

#' Generate SPSP submission files.
#' @param args Program arguments.
main <- function(args) {
  date <- make_outdir(args)
  db_connection <- connect_to_db(args)
  test_sampleset_dir(args)
  db_output <- get_samples_to_release(db_connection, args)
  if (length(db_output$samples) > 0) {
    config <- read_yaml(file = args$config)
    raw_data_file_names <- upload_raw_data_files(
      db_output$samples,
      date,
      config$raw_data_upload
    )
    metadata_list <- get_sample_metadata(
      db_connection,
      args,
      db_output$samples,
      raw_data_file_names
    )
    new_notes <- rbind(db_output$new_notes, metadata_list$new_notes)
    write_new_notes(db_connection, new_notes)
    metadata <- metadata_list$metadata
    frameshifts_tbl <- get_frameshift_diagnostics(db_connection, metadata, args)
    write_out_files(
      metadata,
      frameshifts_tbl,
      db_output$summary,
      db_connection, args
    )
  } else {
    submission_dir <- paste(
      args$outdir,
      "for_submission/viruses",
      Sys.Date(),
      sep = "/"
    )
    system(command = paste("rm -rf", submission_dir))
    print(log.info(
      msg = "No samples to release.",
      fcn = paste0(args$script_name, "::", "main")
    ))
  }
}

#' Create output directory.
make_outdir <- function(args) {
  date <- Sys.Date()
  submission_dir <- paste(
    args$outdir,
    "for_submission/viruses",
    date,
    sep = "/"
  )
  sent_dir <- paste(args$outdir, "for_submission/sent", sep = "/")
  if (dir.exists(submission_dir)) {
    print(log.error(
      msg = paste(
        "Specified outdir",
        args$outdir,
        "already has a viruses directory for today. Will not overwrite."
      ),
      fcn = paste0(args$script_name, "::", "make_outdir")
    ))
    stop()
  } else {
    system(command = paste("mkdir -p", submission_dir))
    system(command = paste("mkdir -p", sent_dir))
    print(log.info(
      msg = "Created outdir for submission files.",
      fcn = paste0(args$script_name, "::", "make_outdir")
    ))
  }
  return(date)
}

#' Connect to database.
#' @return Database connection.
connect_to_db <- function(args) {
  db_connection <- tryCatch(
    {
      print(log.info(
        msg = "Connecting to database.",
        fcn = paste0(args$script_name, "::", "connect_to_db")
      ))
      open_database_connection(
        db_instance = "server",
        config_file = "config.yml"
      )
    },
    error = function(cond) {
      json_error <- log.error(
        msg = "Cannot connect to database.",
        fcn = paste0(args$script_name, "::", "connect_to_db")
      )
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
      msg = paste(
        "Specified samplesetdir",
        args$samplesetdir,
        "does not exist."
      ),
      fcn = paste0(args$script_name, "::", "test_sampleset_dir")
    ))
    stop()
  } else {
    print(log.info(
      msg = "Checked that samplesetdir exists.",
      fcn = paste0(args$script_name, "::", "test_sampleset_dir")
    ))
  }
}

#' Get samples to release to SPSP by querying the database.
#' @return List, where first entry is vector of sample names to release and
#' second entry is dataframe summarizing why other sequences in database not
#' released.
get_samples_to_release <- function(db_connection, args) {
  print(log.info(
    msg = "Querying database for samples to release.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))
  finalized_batches <- unname(
    unlist(
      dplyr::tbl(db_connection, "sequencing_batch_status") %>%
      filter(finalized_status) %>%
      select(sequencing_batch) %>%
      collect()
    )
  )
  all_db_seqs <-
    dplyr::tbl(db_connection, "consensus_sequence") %>%
    select(
      sample_name,
      ethid,
      sequencing_center,
      sequencing_batch,
      sequencing_plate,
      sequencing_plate_well
    ) %>%
    collect()

  all_db_seqs_additional <-
    dplyr::tbl(db_connection, "consensus_sequence_meta") %>%
    select(
      sample_name,
      consensus_n,
      diagnostic_number_n,
      qc_result
    ) %>%
    collect()
  all_db_seqs <- left_join(
    x = all_db_seqs,
    y = all_db_seqs_additional,
    by = "sample_name"
  )
  all_db_seqs_notes <-
    dplyr::tbl(db_connection, "consensus_sequence_notes") %>%
    select(sample_name, release_decision) %>%
    collect()
  all_db_seqs <- left_join(
    x = all_db_seqs,
    y = all_db_seqs_notes,
    by = "sample_name"
  )

  print(log.info(
    msg = "Checking if batches are fully loaded into database.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))
  batch_completeness <- check_all_seqs_imported(
    batches = unique(all_db_seqs$sequencing_batch)[
      !is.na(unique(all_db_seqs$sequencing_batch))
    ],
    samples_in_database = all_db_seqs$sample_name,
    sampleset_dir = args$samplesetdir,
    verbose = F
  )
  incomplete_batches <- names(batch_completeness)[!(batch_completeness)]

  print(log.info(
    msg = paste(
      "Checking if sequence in database consensus_n matches",
      "sequence on D-BSSE server number_n."
    ),
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))
  seq_discrepencies <- all_db_seqs %>%
    filter(diagnostic_number_n != consensus_n)

  print(log.info(
    msg = "Checking if sequences previously submitted or released.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))
  released <- dplyr::tbl(db_connection, "sequence_identifier") %>%
    filter(
      !is.na(gisaid_uploaded_at) |
      !is.na(gisaid_id) |
      !is.na(spsp_uploaded_at)
    ) %>%
    collect()

  print(log.info(
    msg = "Checking if sequences have associated metadata in test_metadata.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))
  has_test_metadata <- unlist(
    dplyr::tbl(db_connection, "test_metadata") %>% select(ethid) %>% collect()
  )
  has_metadata <- all_db_seqs$ethid[(all_db_seqs$ethid %in% has_test_metadata)]

  print(log.info(
    msg = "Annotating sequences in database with reasons not to release, if any.",
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))

  all_db_seqs_annotated <- all_db_seqs %>%
    mutate(first_pass_no_fail = qc_result == "no fail reason") %>%
    group_by(ethid, first_pass_no_fail) %>%
    arrange(consensus_n, .by_group = T) %>%
    mutate(
      duplicate_idx = row_number(),
      qc_result = case_when(
        !(sequencing_batch %in% finalized_batches) ~
          paste(
            "sequencing batch not finalized according to",
            "table sequencing_batch_status"
          ),
        release_decision ~
          "column release_decision in consensus_sequence is true",
        # TODO: check if I need to go with with TEST_ID
        ethid %in% released$ethid ~ "ethid already released or submitted",
        !(ethid %in% has_metadata) ~ "no metadata in test_metadata",
        qc_result == "no fail reason" & is.na(ethid) ~ "null ethid",
        qc_result == "no fail reason" & duplicate_idx > 1 ~
          "less complete duplicate",
        sample_name %in% seq_discrepencies$sample_name ~
          "sequence discrepency between D-BSSE server and database",
        T ~ qc_result
      ),
      warning_reason = case_when(
        sequencing_batch %in% incomplete_batches ~
        "data from batch not completely loaded into database"
      )
    ) %>%
    ungroup()

  fail_reason_summary <- summarize_fail_reasons(all_db_seqs_annotated)
  report_suspicious_batches(fail_reason_summary, args)
  report_null_ethids(all_db_seqs_annotated, args)
  all_db_seqs_annotated <- report_controls_with_ethid(
    all_db_seqs_annotated,
    args
  )

  to_release <- all_db_seqs_annotated %>% filter(qc_result == "no fail reason")
  to_release <- report_resequenced_samples(to_release, db_connection, args)
  print(log.info(
    msg = paste(
      "Found", nrow(to_release[[1]]),
      "sequences in database to release."
    ),
    fcn = paste0(args$script_name, "::", "get_samples_to_release")
  ))
  output <- list(
    "samples" = data.frame(
      sample_name = to_release[[1]]$sample_name,
      ethid = to_release[[1]]$ethid,
      is_reseq = to_release[[1]]$is_reseq,
      first_submit_ethid = to_release[[1]]$first_submit_ethid
    ),
    "summary" = fail_reason_summary,
    "new_notes" = to_release[[2]]
  )
  # print(output)
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
        format = "%Y%m%d"
      ),
      batch_week = format(batch_date, "%Y-%W")
    ) %>%
    arrange(
      desc(batch_week),
      sequencing_center,
      sequencing_batch,
      desc(frac_batch)
    )
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
        (qc_result == "ethid already released or submitted"
         & frac_batch < 0.8)
        | warning_reason == "data from batch not completely loaded into database"
        | qc_result == "no fail reason" & frac_batch < 0.8
      ) %>%
      select(sequencing_batch)
  )
  suspicious_batch_summary <- fail_reason_summary %>%
    filter(sequencing_batch %in% suspicious_batches)
  if (nrow(suspicious_batch_summary) > 1) {
    suspicious_batch_message <- paste(
      "Some batches have suspicious QC values:",
      format_dataframe_for_log(
        suspicious_batch_summary %>%
          mutate(seq_count = paste(seq_count, "samples")) %>%
          select(
            sequencing_center,
            sequencing_batch,
            qc_result,
            warning_reason,
            seq_count
          )
      ),
      sep = "\n"
    )
    print(notify.warn(
      msg = suspicious_batch_message,
      fcn = paste0(args$script_name, "::", "report_suspicious_batches")
    ))
  } else {
    print(log.info(
      msg = "Checked that QC for batches in last 3 weeks looks okay.",
      fcn = paste0(args$script_name, "::", "report_suspicious_batches")
    ))
  }
}

#' Warn when no ETHID is found (these should be controls).
report_null_ethids <- function(all_db_seqs_annotated, args) {
  null_ethid_summary <- all_db_seqs_annotated %>%
    filter(qc_result == "null ethid") %>%
    select(sequencing_center, sequencing_batch, sample_name)
  if (nrow(null_ethid_summary) > 1) {
    null_ethid_message <- paste(
      "ETHID not parsed from some sample names (tail shown).",
      "These assumed to be controls and not released:",
      format_dataframe_for_log(tail(null_ethid_summary)),
      sep = "\n"
    )
    print(notify.warn(
      msg = null_ethid_message,
      fcn = paste0(args$script_name, "::", "report_null_ethids")
    ))
  } else {
    print(log.info(
      msg = "Checked that ETHIDs successfully parsed from all sample names.",
      fcn = paste0(args$script_name, "::", "report_null_ethids")
    ))
  }
}

# Remove and report any control with ETHID. This can happen if a lab sends
# out an internal ctrl for sequencing within their plate
report_controls_with_ethid <- function(all_db_seqs_annotated, args) {
  print(colnames(all_db_seqs_annotated))
  sample_name <- all_db_seqs_annotated$sample_name
  controls <- grep("control", sample_name, ignore.case = TRUE)
  if (length(controls) > 0) {
    print(notify.warn(
      msg = paste0(
        "Removing ", length(controls),
        " control samples with ethid: ", paste(controls, collapse = ",")
      ),
      fcn = paste0(args$script_name, "::", "report_controls_with_ethid")
    ))
    non_ctrls <- grep("control", sample_name, ignore.case = TRUE, invert = TRUE)
    return(all_db_seqs_annotated[non_ctrls, ])
  }
}

# Remove and report any sample that is a re-sequencing and has NOT been
# submitted already.
# We still need to submit re-sequencing of non-submitted samples because in
# that case the new sample may be done to solve the failure reason
report_resequenced_samples <- function(to_release, db_connection, args) {
  involved_labs <- strsplit(
    x = read_yaml(file = args$config)[["involved_labs"]],
    split = " ")[[1]]
  labs_meta <- NULL
  for (i in 1:length(involved_labs)) {
    labs_meta <- rbind(
      labs_meta,
      dplyr::tbl(db_connection, paste0(involved_labs[i], "_metadata")) %>%
        select(ethid, sample_number) %>%
        collect()
    )
  }
  submitted <- dplyr::tbl(db_connection, "sequence_identifier") %>%
    select(ethid, spsp_uploaded_at, gisaid_uploaded_at) %>%
    collect()
  submitted <- left_join(submitted, labs_meta, by = "ethid")
  # on_spsp <- which(!is.na(submitted$spsp_uploaded_at))
  # on_gisaid <- which(!is.na(submitted$gisaid_uploaded_at))
  # tmp <- unique(c(on_spsp, on_gisaid))
  # submitted <- submitted[tmp,]
  to_release_sn <- left_join(to_release, labs_meta, by = "ethid")


  cons_meta <- dplyr::tbl(db_connection, "consensus_sequence_meta") %>%
    select("sample_name", "consensus_n") %>%
    collect()
  cons <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    select("ethid", "sample_name") %>%
    collect()
  to_release_sn$is_reseq <- FALSE
  to_release_sn$is_reseq[
    which(to_release_sn$sample_number %in% submitted$sample_number)
  ] <- TRUE
  to_release_sn_final <- NULL
  to_ignore <- NULL
  if (nrow(to_release_sn) >= 1) {
    for (i in 1:nrow(to_release_sn)) {
      if (to_release_sn$is_reseq[i]) {
        if (is.na(to_release_sn$sample_number[i])) {
          to_ignore <- rbind(to_ignore, to_release_sn[i, ])
          next()
        }
        first_submit_ethid <- submitted$ethid[
          which(submitted$sample_number %in% to_release_sn$sample_number[i])
        ]
        first_submit <- cons[which(cons$ethid %in% first_submit_ethid), ]
        first_submit <- left_join(first_submit, cons_meta)
        first_submit <- first_submit[
          which(first_submit$consensus_n == min(first_submit$consensus_n)),
        ]

        if (to_release_sn$consensus_n[i]/min(first_submit$consensus_n) < 0.9) {
          myline <- to_release_sn[i, ]
          myline$first_submit_ethid <- first_submit_ethid
          to_release_sn_final <- rbind(to_release_sn_final, myline)
        } else {
          to_ignore <- rbind(to_ignore, to_release_sn[i, ])
        }
      } else {
        myline <- to_release_sn[i, ]
        myline$first_submit_ethid <- NA
        to_release_sn_final <- rbind(to_release_sn_final, myline)
      }
    }
  }

  print(log.info(
    msg = paste0(
      "Detected ", length(to_release_sn$is_reseq),
      " resequencings. Of them, ", length(to_release_sn_final$is_reseq),
      "are at least 10% better in quality (#Ns in the consensus) ",
      "and will be submitted as an update: "),
    fcn = paste0(args$script_name, "::", "report_resequenced_samples")
  ))



  if (!is.null(to_ignore)) {
    print(log.info(
      msg = paste(
        "Updating database to ignore the worse-quality resequencing",
        "and logging the reason"
      ),
      fcn = paste0(args$script_name, "::", "report_resequenced_samples")
    ))

    new_notes = data.frame(
      sample_name = to_ignore$sample_name,
      release_decision = TRUE,
      comment = "discarded re-sequencing (worse qc or NA sample_number")
  } else {
    new_notes = NULL
  }
  return(list(to_release_sn_final, new_notes))
}


#' Query database to assemble sequence metadata.
get_sample_metadata <- function(db_connection, args, samples,
                                raw_data_file_names) {
  print(log.info(
    msg = "Querying database for sample metadata.",
    fcn = paste0(args$script_name, "::", "get_sample_metadata")
  ))
  query_seqs <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(sample_name %in% !!samples$sample_name) %>%
    select(sample_name, sequencing_plate, sequencing_plate_well,
           sequencing_center, sequencing_batch, ethid) %>%
    collect()
  query_seqs_additional <-
    dplyr::tbl(db_connection, "consensus_sequence_meta") %>%
    select(sample_name, coverage_mean) %>%
    collect()
  query_seqs <- left_join(
    x = query_seqs,
    y = query_seqs_additional,
    by = "sample_name")
  #  all_plates_mapping <- dplyr::tbl(db_connection, "test_plate_mapping") %>%
  #    select(test_id, sequencing_plate, sequencing_plate_well) %>%
  #    collect()
  #  query_seqs <- left_join(
  #     x = query_seqs,
  #     y = all_plates_mapping,
  #     by = c("sequencing_plate", "sequencing_plate_well"),
  #     na_matches="never"
  #  )

  ## "filter" with %in% does not work on data frame columns, so I need to use
  # a temporary variable
  ethid_to_release <- query_seqs$ethid
  # join sequence data to sample metadata using test_id
  query_main_meta <- dplyr::tbl(db_connection, "test_metadata") %>%
    select(ethid, test_id, order_date, zip_code, purpose) %>%
    filter(ethid %in% ethid_to_release) %>%
    collect()

  if (nrow(query_main_meta) != length(ethid_to_release)) {
    print(log.error(
      msg = "FATAL: some samples are missing metadata in table test_metadata",
      fcn = paste0(args$script_name, "::", "get_sample_metadata")
    ))
    stop()
  }

  query_imv <- dplyr::tbl(db_connection, "imv_metadata") %>%
    filter(ethid %in% ethid_to_release) %>%
    collect()
  query_eoc <- dplyr::tbl(db_connection, "eoc_metadata") %>%
    filter(ethid %in% ethid_to_release) %>%
    collect()
  query_viollier <- dplyr::tbl(db_connection, "viollier_metadata") %>%
    filter(ethid %in% ethid_to_release) %>%
    collect()
  query_teamw <- dplyr::tbl(db_connection, "teamw_metadata") %>%
    filter(ethid %in% ethid_to_release) %>%
    collect()

  # Check and report the number found for each lab
  print(log.info(
    msg = paste(
      "Found:",
      nrow(query_eoc), "EOC samples;",
      nrow(query_imv), "IMV samples;",
      nrow(query_viollier), "Viollier samples;",
      nrow(query_teamw), "TeamW samples"
    ),
    fcn = paste0(args$script_name, "::", "get_sample_metadata")
  ))

  if (nrow(query_eoc) + nrow(query_imv) +
      nrow(query_viollier) + nrow(query_teamw)
      != length(ethid_to_release)) {
    print(log.error(
      msg = paste(
        "FATAL: some samples are missing lab-specific metadata",
        "in the respective <lab>_metadata table"
      ),
      fcn = paste0(args$script_name, "::", "get_sample_metadata")
    ))
  }

  # Create a dictionary for assigning the sample name
  tmp <- rbind(query_imv[, 1:2], query_eoc[, 1:2],
               query_teamw[, 1:2], query_viollier[, 1:2])
  sample_number_dict <- tmp$sample_number
  names(sample_number_dict) <- tmp$ethid

  # Create and fill covv_orig_lab based on the lab of origin, to retrieve
  # the lab_code_foph from the database
  query_main_meta <- query_main_meta %>%
    mutate(
      covv_orig_lab = case_when(
        ethid %in% query_eoc$ethid ~ "EOC Bellinzona",
        ethid %in% query_imv$ethid ~
          "Institute of Medical Virology, University of Zurich",
        ethid %in% query_teamw$ethid ~ "labor team w AG",
        ethid %in% query_viollier$ethid ~ "Viollier AG"
      ),
      covv_orig_lab_addr = case_when(
        ethid %in% query_eoc$ethid ~ "Via Mirasole 22/A, 6500 Bellinzona",
        ethid %in% query_imv$ethid ~ "IMV",
        ethid %in% query_teamw$ethid ~ "Blumeneggstrasse 55, 9403 Goldach",
        ethid %in% query_viollier$ethid ~ "Hagmattstrasse 14, 4123 Allschwil"
      ),
      sample_number = recode(
        ethid,
        !!!sample_number_dict,
        .default = NA_character_
      )
    ) %>%
    collect()

  join_w_reason <- left_join(x = query_seqs, y = query_main_meta, by = "ethid")

  query_canton_code <-
    dplyr::tbl(db_connection, "swiss_postleitzahl") %>%
      # get canton based on zip_code in metadata since canton is sometimes
      # missing
    select(plz, canton) %>%
    rename(zip_code = plz) %>%
    collect()
  seq_metadata <- left_join(
    x = join_w_reason,
    y = query_canton_code,
    by = "zip_code"
  )

  seq_metadata <- seq_metadata %>% dplyr::left_join(
    y = dplyr::tbl(db_connection, "lab_code_foph") %>% collect(),
    by = "covv_orig_lab",
    na_matches = "never"
  ) # join collecting_lab_code

  canton_fullname <-
    dplyr::tbl(db_connection, "swiss_canton") %>% # get full canton names
    select(canton_code, english) %>%
    rename(canton = canton_code, canton_fullname = english) %>%
    collect()

  if (length(raw_data_file_names$r1_files) > 0) {
    r1_files <- stack(raw_data_file_names$r1_files)
    names(r1_files) <- c("orig_fastq_name_forward", "sample_name")
    seq_metadata <- merge(
      x = seq_metadata,
      y = r1_files,
      by = "sample_name",
      all.x = T
    )
    seq_metadata$orig_fastq_name_forward[
      is.na(seq_metadata$orig_fastq_name_forward)
    ] <- ""
  } else {
    seq_metadata$orig_fastq_name_forward <- ""
    print(log.warn(
      msg = paste(
        "Warning: the metadata will have an empty field for the ",
        "forward raw fastq files. Please manually upload an update",
        "with the raw data in the future"
      ),
      fcn = paste0(args$script_name, "::", "get_sample_metadata")
    ))
  }
  if (length(raw_data_file_names$r2_files) > 0) {
    r2_files <- stack(raw_data_file_names$r2_files)
    names(r2_files) <- c("orig_fastq_name_reverse", "sample_name")
    seq_metadata <- merge(
      x = seq_metadata,
      y = r2_files,
      by = "sample_name",
      all.x = T
    )
    seq_metadata$orig_fastq_name_reverse[
      is.na(seq_metadata$orig_fastq_name_reverse)
    ] <- ""
  } else {
    seq_metadata$orig_fastq_name_reverse <- ""
    print(log.warn(
      msg = paste(
        "Warning: the metadata will have an empty field for the reverse ",
        "raw fastq files. Please manually upload an update with the ",
        "raw data in the future"
      ),
      fcn = paste0(args$script_name, "::", "get_sample_metadata")
    ))
  }
  metadata_w_canton <- merge(
    x = seq_metadata, y = canton_fullname, by = "canton", all.x = T
  )

  metadata_w_reseq <- left_join(metadata_w_canton, samples)

  qcd_metadata <- qc_sample_metadata(metadata = metadata_w_reseq, args)
  spsp_formatted_metadata <- format_metadata_for_spsp(
    metadata = qcd_metadata,
    args
  )
  added_metadata <- insert_additional_metadata(
    spsp_formatted_metadata,
    args,
    db_connection
  )
  ovverridden_metadata <- override_strain_names_update(
    added_metadata,
    args,
    db_connection,
    samples
  )
  metadata <- check_mandatory_columns(metadata = overridden_metadata, args)
  return(metadata)
}

#' Check metadata for required information.
qc_sample_metadata <- function(metadata, args) {
  # Are any sequences missing the Canton? If so, replace with "UN" for "unknown".
  missing_canton_info <- metadata %>% filter(is.na(canton))
  if (nrow(missing_canton_info) > 0) {
    print(log.info(
      msg = paste(
        nrow(missing_canton_info),
        "samples are missing Canton information.",
        "Replacing with 'UN' for 'unknown'."
      ),
      fcn = paste0(args$script_name, "::", "qc_sample_metadata")
    ))
    metadata <- metadata %>%
      mutate(canton = tidyr::replace_na(data = canton, replace = "UN"))
  }
  # Do we have any duplicate sequences of the same sample_number?
  temp <- metadata %>%
    collect %>%
    group_by(sample_number) %>%
    filter(n() > 1)
  if (nrow(temp) > 0 & !all(is.na(temp$sample_number))) {
    print(notify.error(
      msg = paste(
        "Some sequences associated with same sample_number:",
        format_dataframe_for_log(
          temp %>%
          ungroup() %>%
          select(sample_name, ethid)
        ),
        sep = "\n"
      ),
      fcn = paste0(args$script_name, "::", "qc_sample_metadata")
    ))
    stop()
  }
  # Do we have any duplicate sequences of the same ethid?
  temp <- metadata %>%
    collect %>%
    group_by(ethid) %>%
    filter(n() > 1)
  if (nrow(temp) > 0) {
    print(notify.error(
      msg = paste(
        "Some sequences associated with same ethid:",
        format_dataframe_for_log(
          temp %>%
          ungroup() %>%
          select(sample_name, ethid)
        ),
        sep = "\n"
      ),
      fcn = paste0(args$script_name, "::", "qc_sample_metadata")
    ))
    stop()
  }
  return(metadata)
}

#' Format metadata for SPSP.
format_metadata_for_spsp <- function(metadata, args) {
  print(log.info(
    msg = "Formatting metadata for SPSP.",
    fcn = paste0(args$script_name, "::", "format_metadata_for_spsp")
  ))
  seq_authors <- read_seq_authors(args)
  metadata$lab_short_name <- unlist(
    lapply(
      strsplit(metadata$test_id, "/"), function(x) x[1]
    )
  )
  metadata$authors = NULL
  for (i in 1:nrow(metadata)) {
    if (metadata$covv_orig_lab[i]
        == "Institute of Medical Virology, University of Zurich") {
      metadata$authors[i] = seq_authors$imv
    }
    if (metadata$covv_orig_lab[i] == "EOC Bellinzona") {
      metadata$authors[i] = seq_authors$eoc
    }
    if (metadata$covv_orig_lab[i] == "labor team w AG") {
      metadata$authors[i] = seq_authors$teamw
    }
    if (metadata$covv_orig_lab[i] == "Viollier AG") {
      metadata$authors[i] = seq_authors$viollier
    }

    if (metadata$sequencing_center[i] == "viollier") {
      metadata$authors[i] = paste(
        metadata$authors[i],
        seq_authors$viollier,
        sep = ", "
      )
    }
    if (metadata$sequencing_center[i] == "gfb") {
      metadata$authors[i] = paste(
        metadata$authors[i],
        seq_authors$gfb,
        sep = ", "
      )
    }
    if (metadata$sequencing_center[i] == "h2030") {
      metadata$authors[i] = paste(
        metadata$authors[i],
        seq_authors$h2030,
        sep = ", "
      )
    }
    if (metadata$sequencing_center[i] == "fgcz") {
      metadata$authors[i] = paste(
        metadata$authors[i],
        seq_authors$fgcz,
        sep = ", "
      )
    }
    metadata$authors[i] = paste(
      metadata$authors[i],
      seq_authors$ethz,
      sep = ", "
    )
  }

  metadata_for_spsp <- metadata %>%
    summarise(
      orig_fasta_name = paste0(sample_name, ".fasta.gz"),
      strain_name = case_when(
        lab_short_name == "imv" ~ paste(
          "hCoV-19", "Switzerland",
          paste(canton, "UZH-IMV", "PLACEHOLDER", sep = "-"),
          lubridate::year(order_date),
          sep = "/"
        ),
        lab_short_name == "eoc" ~ paste(
          "hCoV-19", "Switzerland",
          paste(canton, "EOC", ethid, sep = "-"),
          lubridate::year(order_date),
          sep = "/"
        ),
        lab_short_name == "viollier" ~ paste(
          "hCoV-19", "Switzerland",
          paste(canton, "ETHZ", ethid, sep = "-"),
          lubridate::year(order_date),
          sep = "/"
        ),
        lab_short_name == "teamw" ~ paste(
          "hCoV-19", "Switzerland",
          paste(canton, "ETHZ", ethid, sep = "-"),
          lubridate::year(order_date),
          sep = "/"
        )
      ),
      is_assembly_update = case_when(
        is_reseq == TRUE ~ "yes",
        is_reseq == FALSE ~ "no"
      ),
      isolation_date = order_date,
      location_general = case_when(
        is.na(canton_fullname) ~ paste("Europe", "Switzerland", sep = " / "),
        T ~ paste("Europe", "Switzerland", canton_fullname, sep = " / ")
      ),
      isolation_source_description = "Human",
      host_sex = "Unknown",
      host_age = "Unknown",
      isolation_source_detailed = "Respiratory specimen",
      library_preparation_kit = case_when(
        sequencing_center == "viollier" ~ "Illumina_COVIDSeq (ARTIC V4)",
        sequencing_center == "gfb" ~ "NEB (ARTIC V3)",
        sequencing_center == "h2030" ~ "Illumina_COVIDSeq (ARTIC V4)",
        sequencing_center == "fgcz" ~ "Nextera XT (ARTIC V4)",
        sequencing_center == "fgcz" & as.Date(
          gsub(sequencing_batch, pattern = "_.*", replacement = ""),
          format = "%Y%m%d"
        ) < as.Date("2021-04-19") ~ "NEB"
      ),
      sequencing_platform =
        "Combination Illumina MiSeq and Illumina NovaSeq 5000/6000",
      assembly_method = "V-pipe",
      raw_dataset_coverage = round(coverage_mean, digits = 0),
      reporting_lab_name = case_when(
        lab_short_name == "imv" ~ paste(
          "Institute of Medical Virology,",
          "University of Zurich, Zurich, Switzerland /",
          "Department of Biosystems Science and Engineering,",
          "ETH Zürich; Mattenstrasse 26, 4058 Basel"
        ),
        TRUE ~ paste(
          "Department of Biosystems Science and Engineering,",
          "ETH Zürich; Mattenstrasse 26, 4058 Basel"
        )
      ),
      reporting_lab_order_id = sample_name,
      collecting_lab_name = paste(
        covv_orig_lab,
        covv_orig_lab_addr,
        sep = "; "
      ),
      collecting_lab_code = lab_code_foph,
      collecting_lab_order_id = sample_number,
      sequencing_lab_name = case_when(
        sequencing_center == "gfb" ~ "Genomic Facility Basel",
        sequencing_center %in%
          c("fgcz", "fcgz") ~ "Functional Genomics Center Zurich",
        sequencing_center == "h2030" ~ "H2030 Genome Center",
        sequencing_center == "viollier" ~ "Viollier AG"
      ),
      reporting_authors = authors,
      sequencing_purpose = purpose,
      orig_fastq_name_forward = orig_fastq_name_forward,
      orig_fastq_name_reverse = orig_fastq_name_reverse,

      # This column indicates if we are submitting on behalf of someone else.
      # At the time of writing (July 29th 2022), only IMV and EOC requested
      # a "on_behalf" submission. The content of the column is communicated
      # to us by SPSP. The last communication was on July 8th 2022
      on_behalf = case_when(
        lab_short_name == "imv" ~ "UZH-MedVir",
        lab_short_name == "eoc" ~ "EOC-LM"
      )
    )

  rownames(metadata_for_spsp) <- metadata$sample_name
  return(metadata_for_spsp)
}

#' Get author lists for each sequencing center.
read_seq_authors <- function(args) {
  print(log.info(
    msg = paste("Reading sequence authors from config file", args$config),
    fcn = paste0(args$script_name, "::", "read_seq_authors")
  ))
  seq_authors <- read_yaml(file = args$config)
  return(seq_authors$authors)
}

# Add the information that labs send to us manually
insert_additional_metadata <- function(metadata, args, db_connection) {
  involved_labs <- strsplit(
    x = read_yaml(file = args$config)[["involved_labs"]],
    split = " "
  )[[1]]
  labs_meta <- list()
  ethids <- unlist(
    lapply(
      strsplit(metadata$reporting_lab_order_id, "--"),
      function(x) x[1]
    )
  )

  for (i in 1:length(involved_labs)) {
    if (involved_labs[i] == "teamw") {
      next()
    }
    if (involved_labs[i] == "eoc") {
      next()
    }
    if (involved_labs[i] == "viollier") {
      next()
    }
    if (involved_labs[i] == "imv") {
      labs_meta[[i]] <-
        dplyr::tbl(db_connection,  paste0(involved_labs[i], "_metadata")) %>%
        filter(ethid %in% ethids) %>%
        collect()
      if (nrow(labs_meta[[i]]) == 0) {
        print(log.info(
          msg = paste(
            "Found no IMV samples with additional metadata in imv_metadata.",
            "This should happen only if there are no IMV samples for today",
            args$config
          ),
          fcn = paste0(args$script_name, "::", "insert_additional_metadata")
        ))
        next()
      }
      for (k in 1:nrow(labs_meta[[i]])) {
        meta_row_index <- which(ethids %in% labs_meta[[i]]$ethid[k])

        for (m in 1:ncol(labs_meta[[i]])) {
          if (colnames(labs_meta[[i]])[m] %in% c("ethid", "sample_number")) {
            next()
          }
          if (colnames(labs_meta[[i]])[m] == "sample_name_anonymised") {
            new_strain <- metadata$strain_name[meta_row_index]
            new_strain <- unlist(strsplit(new_strain, "/"))
            tmp_strain <- unlist(strsplit(new_strain[3], "-"))
            tmp_strain[4] <- labs_meta[[i]][k, m]
            tmp_strain <- paste0(tmp_strain, collapse = "-")
            new_strain <- paste(
              new_strain[1], new_strain[2],
              tmp_strain, new_strain[4],
              sep = "/"
            )
            metadata$strain_name[meta_row_index] <- new_strain
          } else {
            meta_col_index <- which(
              colnames(metadata) %in% colnames(labs_meta[[i]])[m]
            )
            metadata[meta_row_index, meta_col_index] <- labs_meta[[i]][k, m]
          }
        }
      }
    }
  }
  return(metadata)
}

# If a sample needs to be updated, do not create a new strain name based on the
override_strain_names_update <- function(metadata, args,
                                         db_connection, samples) {
  metadata <- left_join(
    metadata,
    samples,
    by = c("reporting_lab_order_id" = "sample_name")
  )
  seqid <- dplyr::tbl(db_connection, "sequence_identifier") %>%
    select("ethid", "strain_name") %>%
    collect()
  cons <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    select("ethid", "sample_name") %>%
    collect()
  metadata <- left_join(
    metadata,
    cons,
    by = c("first_submit_ethid" = "ethid"),
    na_matches = "never"
  )
  new_cons_notes <- NULL
  for (i in 1:nrow(metadata)) {
    if (metadata$is_reseq[i]) {
      original_strain_name <- seqid$strain_name[
        which(seqid$ethid == metadata$first_submit_ethid[i])
      ]
      if (is.na(original_strain_name)) {
        stop(paste(
          "ERROR: original strain name not set.",
           "Please correct in the database"
        ))
      }
      or_canton <- unlist(
        lapply(
          strsplit(original_strain_name, "/"),
          function(x) x[3]
        )
      )
      or_canton <- unlist(lapply(strsplit(or_canton, "-"), function(x) x[1]))
      cur_canton <- unlist(
        lapply(
          strsplit(metadata$strain_name[i], "/"),
          function(x) x[3]
        )
      )
      cur_canton <- unlist(lapply(strsplit(cur_canton, "-"), function(x) x[1]))
      if (or_canton != cur_canton) {
        stop(paste(
          "ERROR: canton doesn't match in strain name ",
          "between the original sequence and the resequencing"
        ))
      }
      metadata$strain_name[i] <- original_strain_name
      new_cons_notes <- rbind(
        new_cons_notes,
        data.frame(
          sample_name = metadata$sample_name[i],
          release_decision = TRUE,
          comment = paste0("better quality resequencing available: ",
          metadata$ethid[i])
        )
      )
    }
  }
  metadata <- metadata[, which(!colnames(metadata) %in%
    c("ethid", "is_reseq", "first_submit_ethid", "sample_name"))
  ]
  return(list("metadata" = metadata, "new_notes" = new_cons_notes))
}

write_new_notes <- function(db_connection, new_notes) {
  key_col <- c("sample_name")
  table_spec <- parse_table_specification(
    table_name <- "consensus_sequence_notes",
    db_connection = db_connection
  )
  cols_to_update <- c("release_decision", "comment")

  update_table(
    table_name = "consensus_sequence_notes",
    new_table = new_notes,
    con = db_connection,
    append_new_rows = T,
    cols_to_update = cols_to_update,
    key_col = key_col,
    table_spec = table_spec,
    run_summarize_update = T
  )
}

# Check if missing any mandatory metadata.
check_mandatory_columns <- function(metadata, args) {
  print(log.info(
    msg = "Checking metadata has all mandatory columns filled.",
    fcn = paste0(args$script_name, "::", "get_sample_metadata")
  ))
  mandatory_cols <- strsplit(
    x = read_yaml(file = args$config)[["mandatory_metadata_columns"]],
    split = " "
  )[[1]]
  mandatory_data <- metadata %>% select(all_of(mandatory_cols))
  incomplete_data_info <- mandatory_data %>%
    filter(!complete.cases(mandatory_data))

  if (nrow(incomplete_data_info) > 0) {
    print(log.warn(
      msg = paste(
        "Incomplete mandatory metadata for these samples.",
        "They will not be released:",
        format_dataframe_for_log(incomplete_data_info),
        sep = "\n"
      ),
      fcn = paste0(args$script_name, "::", "check_mandatory_columns")
    ))
    metadata <- metadata %>%
      filter(complete.cases(mandatory_data))
  }
  return(metadata)
}

#' Get frameshift diagnostic information.
get_frameshift_diagnostics <- function(db_connection, metadata, args) {
  print(log.info(
    msg = "Querying database for frameshift diagnostic information.",
    fcn = paste0(args$script_name, "::", "get_frameshift_diagnostics")
  ))
  metadata$sample_name <- rownames(metadata)
  frameshifts_tbl <-
    dplyr::tbl(db_connection, "frameshift_deletion_diagnostic") %>%
    filter(sample_name %in% !!metadata$sample_name) %>%
    select(sample_name, indel_position, indel_diagnosis) %>%
    collect()

  without_frameshifts <- metadata %>%
    filter(!sample_name %in% !!frameshifts_tbl$sample_name) %>%
    select(sample_name) %>%
    collect()
  missing_table <- NULL

  print(log.info(
    msg = "Testing if samples without frameshifts have a report available.",
    fcn = paste0(args$script_name, "::", "get_frameshift_diagnostics")
  ))

  for (sample in without_frameshifts$sample_name) {
    missing_table <- c(
      missing_table,
      test_frameshift_table(sample, args, db_connection)
    )
  }
  if (!is.null(missing_table)) {
    print(log.error(
      msg = paste(
        "No frameshift table in sequence:",
        missing_table,
        sep = "\n"
      ),
      fcn = paste0(args$script_name, "::", "without_frameshifts")
    ))
    stop(paste(
      "Fatal: Frameshift table does not exist for the following samples\n",
      missing_table
    ))
  }

  colnames(frameshifts_tbl)[
    which(colnames(frameshifts_tbl) == "indel_position")
  ] <- "indel_position_english"
  n_seqs_with_frameshifts <- length(unique(frameshifts_tbl$sample_name))

  print(log.info(
    msg = "Building the frameshift summary and table.",
    fcn = paste0(args$script_name, "::", "get_frameshift_diagnostics")
  ))

  frameshift_summary <- frameshifts_tbl %>%
    group_by(indel_diagnosis) %>%
    summarize(n_dels = n()) %>%
    arrange(desc(n_dels))

  frameshifts_tbl <- merge(
    x = frameshifts_tbl,
    y = metadata[c("sample_name", "strain_name")],
    all.x = T, all.y = T, by = "sample_name"
  )

  frameshifts_tbl <- frameshifts_tbl %>%
    mutate(indel_position_english =
      tidyr::replace_na(
        data = indel_position_english,
        replace = "no frameshifts found in this sequence"
      )
    )

  frameshifts_tbl <-
    frameshifts_tbl[, which(colnames(frameshifts_tbl) != "sample_name")]
  return(frameshifts_tbl)
}

#' Check if the samples without frameshifts have an associated framshift table
test_frameshift_table <- function(mysample, args, db_connection) {
  sample_batch <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(sample_name %in% !!mysample) %>%
    select(sample_name, sequencing_batch) %>%
    collect()

  print(log.info(
    msg = paste0(
      args$workingdir, "/", mysample, "/", sample_batch$sequencing_batch,
      "/references/frameshift_deletions_check.tsv"
    ),
    fcn = paste0(args$script_name, "::", "get_frameshift_diagnostics")
  ))

  if (!(file.exists(
    paste0(
      args$workingdir, "/", mysample, "/", sample_batch$sequencing_batch,
      "/references/frameshift_deletions_check.tsv"
    )
  ))) {
    return(mysample)
  } else {
    return(NULL)
  }
}


#' Write out files for submission to SPSP.
write_out_files <- function(metadata, frameshifts_tbl, batches_summary,
                           db_connection, args) {
  print(log.info(
    msg = "Writing out files for submission.",
    fcn = paste0(args$script_name, "::", "write_out_files")
  ))
  submission_dir <- paste(
    args$outdir,
    "for_submission/viruses",
    Sys.Date(),
    sep = "/"
  )
  write.table(
    x = metadata,
    file = paste(
      submission_dir,
      paste0("metadata-file-", Sys.Date(), ".tsv"), sep = "/"
    ),
    sep = "\t",
    row.names = F
  )

  write.table(
    x = frameshifts_tbl,
    file = paste(
      submission_dir,
      paste0("ETHZ-frameshift_report-", Sys.Date(), ".tsv"), sep = "/"
    ),
    sep = "\t",
    quote = F,
    row.names = F
  )

  write.table(
    x = batches_summary,
    file = paste(args$outdir, "batches_summary.tsv", sep = "/"),
    sep = "\t",
    quote = F,
    row.names = F
  )

  header_mapping <- metadata$strain_name
  names(header_mapping) <- metadata$reporting_lab_order_id

  export_seqs_as_fasta(
    db_connection = db_connection,
    sample_names = metadata$reporting_lab_order_id,
    seq_outdir = submission_dir,
    overwrite = F,
    header_mapping = header_mapping,
    gzip = T
  )
}

check_raw_data_upload_config <- function(config) {
  required <- c(
    "server", "user", "uploads_folder", "private_key_euler",
    "passphrase", "max_conn", "max_samples_per_call"
  )


  missing <- setdiff(required, names(config))
  unknown <- setdiff(names(config), required)

  errors <- 0
  if (length(missing) > 0) {
    cat(paste(c(
      "error: the following entries in config are missing:", missing, "\n"
    )))
    errors <- errors + 1
  }
  if (length(unknown) > 0) {
    cat(paste(c(
      "error: the following entries in config are not known:", unknown, "\n"
    )))
    errors <- errors + 1
  }


  for (name in required) {
    value <- config[name]
    if (is.null(value))
      next
    if (length(config[name]) == 0) {
      cat(paste("error: config entry for", name, "is empty\n"))
      errors <- errors + 1
    }
  }

  for (num_field in c("max_conn", "max_samples_per_call")) {
    value <- config[name]
    if (is.null(value))
      next
    value <- suppressWarnings(as.integer(value));
    if (is.na(value)) {
      cat(paste(
        "error: config entry for", num_filed, "is not an integer number\n"
      ))
      errors <- errors + 1
      next
    }
    config[num_field] <- value
  }
  if (errors > 0)
    stop("config not valid")
}

check_config <- function(config_file) {
  config <- read_yaml(file = config_file)
  check_raw_data_upload_config(config$raw_data_upload)
}


# Production arguments
parser <- argparse::ArgumentParser()
parser$add_argument(
  "--config",
  type = "character",
  help = "Path to spsp-config.yml.",
  default = "spsp-config.yml"
)
parser$add_argument(
  "--samplesetdir",
  type = "character",
  help = "Path to V-pipe sample directories.",
  default = "vpipe/sampleset"
)
parser$add_argument(
  "--outdir",
  type = "character",
  help = "Path to output files for submission.",
  default = paste("spsp",
  Sys.Date(),
  sep = "/")
)
parser$add_argument(
  "--workingdir",
  type = "character",
  help = "Path to V-pipe working directory.",
  default = "vpipe/working"
)
args <- parser$parse_args()
args[["script_name"]] <- "export_spsp_submission.R"

# Test arguments
# args <- list()
# args[["config"]] <- "../automation/spsp_transferer/database/spsp-config.yml"
# args[["samplesetdir"]] <- "/Volumes/covid19-pangolin/backup/sampleset"
# args[["outdir"]] <- "~/Downloads/test_outdir"
# args[["script_name"]] <- "export_spsp_submission.R"

check_config(args$config)

# Run program
main(args = args)
