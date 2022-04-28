# This script is to run Nextstrain's diagnostic.py script
# https://github.com/nextstrain/ncov/blob/master/scripts/diagnostic.py
# on our Swiss sequences and to load the results into the database table
# 'consensus_sequence.'


# Functions
#' Query the database table 'consensus_sequence' to generate an alignment of the
#' specified samples.
generate_alignment <- function(db_connection, sample_names, seq_outfile, 
                               metadata_outfile, 
                               seq_table_name = "consensus_sequence",
                               sample_name_col = "sample_name",
                               seq_col = "seq_aligned",
                               metadata_table_name = "test_metadata",
                               key_col = "ethid",
                               plate_col = "sequencing_plate",
                               well_col = "sequencing_plate_well",
                               date_col = "order_date",
                               merging_table_name = "test_plate_mapping",
                               main_key = "test_id") {
  for (file in c(seq_outfile, metadata_outfile)) {
    if (file.exists(file)) {
      system(command = paste("rm", file))
    }
  }
  seq_query <- dplyr::tbl(db_connection, seq_table_name) %>%
    filter(!!sym(sample_name_col) %in% sample_names) %>%
    select(!!sym(sample_name_col), !!sym(seq_col), !!sym(key_col), !!sym(plate_col), !!sym(well_col))
  seq_tbl <- seq_query %>% collect()

  merging_table <- dplyr::tbl(db_connection, merging_table_name) %>%
    select(!!sym(main_key), !!sym(plate_col), !!sym(well_col)) %>% collect()

  #seq_tbl <- merge(x = seq_tbl, y = merging_table, all.x = T, all.y = F)
  seq_tbl <- left_join(x=seq_tbl, y=merging_table, by=c(plate_col, well_col), na_matches="never")

  keys <- unlist(seq_tbl[[main_key]])
  metadata_query <- dplyr::tbl(db_connection, metadata_table_name) %>%
    filter(!!sym(main_key) %in% keys) %>%
    select(!!sym(main_key), !!sym(date_col))
  metadata_tbl <- metadata_query %>% collect()
  if (sample_name_col == main_key) {
    selected_cols <- sample_name_col
  } else {
    selected_cols <- c(sample_name_col, main_key)
  }
  metadata_tbl <- merge(x = metadata_tbl, y = seq_tbl[, selected_cols], all.y = T) %>%
    mutate(virus = "ncov", region = "dummy_region", date_submitted = as.Date(!!sym(date_col)) + 14) %>%
    rename(name = !!sym(sample_name_col), date = !!sym(date_col))
  
  seq_outfile_con <- file(seq_outfile, open = "a")
  seq_tbl <- seq_tbl %>% mutate("header" = paste0(">", !!sym(sample_name_col)))
  for (i in seq_len(nrow(seq_tbl))) {
    writeLines(unlist(seq_tbl[i, c("header", seq_col)]), con = seq_outfile_con, sep = "\n")
  }
  close(seq_outfile_con)
  write.table(
    x = metadata_tbl, file = metadata_outfile, sep = "\t", quote = F, row.names = F)
}

run_diagnostic <- function(python_path, ncovdir, alignment, metadata, outdir) {
  for (file in paste(
    outdir, c("diagnostics.txt", "exclusion-list.txt", "flagged.txt"), sep = "/")) {
    if (file.exists(file)) {
      system(command = paste("rm", file))
    }
  }

  script <- paste(ncovdir, "scripts/diagnostic.py", sep = "/")
  reference <- paste(ncovdir, "defaults/reference_seq.gb", sep = "/")
  system(command = paste(
    python_path, script,
    "--alignment", alignment,
    "--reference", reference,
    "--metadata", metadata,
    "--output-diagnostics", paste(outdir, "diagnostics.txt", sep = "/"),
    "--output-flagged", paste(outdir, "flagged.txt", sep = "/"),
    "--output-exclusion-list", paste(outdir, "exclusion-list.txt", sep = "/")))
}

import_diagnostic <- function(db_connection, outdir, tbl_name) {
  # Format data
  diagnostic_transformed <- read.table(
    file = paste(outdir, "diagnostics.txt", sep = "/"), stringsAsFactors = F,
    sep = "\t", fill = T, comment.char = "", header = T, check.names = F) %>%
    rename(
      "number_n" = "#Ns", 
      "number_gaps" = "#gaps",
      "excess_divergence" = "excess divergence")
  flagged <- read.table(
    file = paste(outdir, "flagged.txt", sep = "/"), stringsAsFactors = F,
    sep = "\t", header = T) %>%
      select(strain, flagging_reason)
  diagnostic_transformed <- merge(
    x = flagged, y = diagnostic_transformed, by = "strain", all.y = T)
  colnames(diagnostic_transformed)[colnames(diagnostic_transformed) == "strain"] <- "sample_name"
  diagnostic_transformed$gaps[diagnostic_transformed$gaps == ''] <- NA  # not sure why, but sometimes empty gaps field read as NA and sometimes as ''
  diagnostic_transformed$clusters[diagnostic_transformed$clusters == ''] <- NA  # so that we don't end up with both blanks and nulls in the data
  # Import data
  update_table_internal(
    table_name = tbl_name_meta, new_table = diagnostic_transformed,
    con = db_connection)
}

update_table_internal <- function(table_name, new_table, con) {
  staging_table_name <- paste0(table_name, "_staging")
  if (DBI::dbExistsTable(con, staging_table_name)) {
    DBI::dbRemoveTable(con, staging_table_name)
  }
  
  # create staging table
  DBI::dbWriteTable(con, staging_table_name, new_table)
  
  # Update diagnostic values in table based on values in staging table
  update_sql <- "UPDATE consensus_sequence_meta t
  SET diagnostic_divergence = s.diagnostic_divergence, diagnostic_excess_divergence = s.diagnostic_excess_divergence, diagnostic_number_n = s.diagnostic_number_n, diagnostic_number_gaps = s.diagnostic_number_gaps, diagnostic_clusters = s.diagnostic_clusters, diagnostic_gaps = s.diagnostic_gaps, diagnostic_all_snps = s.diagnostic_all_snps, diagnostic_flagging_reason = s.diagnostic_flagging_reason
  FROM consensus_sequence_meta_staging s WHERE t.sample_name = s.sample_name"
  res <- DBI::dbSendStatement(con, update_sql)
  DBI::dbClearResult(res)
  
  DBI::dbRemoveTable(con, staging_table_name)
}

#'
#' @param db_connection
#' @param outdir
#' @param chunk_size
#' @param update_all_seqs If false, only update table for sequences without any diagnostic stats imported
#' @param ncovdir Cloned from https://github.com/nextstrain/ncov
import_sequence_diagnostic <- function (
  db_connection, outdir = "data/tempdir/", chunk_size = 100, 
  update_all_seqs = FALSE, update_batches = NULL, 
  ncovdir = "python/ncov", python3 = "python3", 
  tbl_name = "consensus_sequence", tbl_name_meta = "consensus_sequence_meta") {

  # Make temporary directory
  if (dir.exists(outdir)) {
    system(command = paste("rm -r", outdir))
    system(command = paste("mkdir -p", outdir))
  } else {
    system(command = paste("mkdir -p", outdir))
  }

  # Query database for sequences without diagnostic QA values yet

  # This query fetches all the sample_names that don't have diagnostic stats
  if (update_all_seqs) {
    query <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      filter(!is.null(seq_aligned)) %>%
      select(sample_name)
  } else if (!is.null(update_batches)) {
    query <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      filter(sequencing_batch %in% update_batches) %>%
      select(sample_name)
  } else {
    query <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      filter(!is.null(seq_aligned)) %>%
      select(sample_name) %>% collect()
    query_meta <- dplyr::tbl(db_connection, "consensus_sequence_meta") %>%
      filter(!is.null(diagnostic_number_n)) %>%
      select(sample_name) %>% collect()
    query <- query$sample_name[query$sample_name %in% query_meta$sample_name]
  }
  sample_names_to_diagnose <- query

  # Because there may be many sequences, run diagnostic on chunks of chunk_size
  i <- 0
  n_samples <- length(sample_names_to_diagnose)
  tmp_aln_file <- paste(outdir, "alignment.fasta", sep = "/")
  tmp_metadata_file <- paste(outdir, "metadata.tsv", sep = "/")
  while (i * chunk_size + 1 <= n_samples) {
    start_sample_idx <- i * chunk_size + 1
    end_sample_idx <- min(i * chunk_size + chunk_size, n_samples)
    sample_names <- sample_names_to_diagnose[start_sample_idx:end_sample_idx]
    print(paste("Operating on samples", start_sample_idx, "to", end_sample_idx, "out of", n_samples))
    i <- i + 1
    
    generate_alignment(
      db_connection = db_connection, sample_names = sample_names,
      seq_outfile = tmp_aln_file, metadata_outfile = tmp_metadata_file)

    run_diagnostic(
      ncovdir = ncovdir, alignment = tmp_aln_file, metadata = tmp_metadata_file,
      outdir = outdir, python_path = python3)

    import_diagnostic(
      db_connection = db_connection, outdir = outdir, tbl_name = tbl_name)
  }

  system(command = paste("rm -r", outdir))
}
