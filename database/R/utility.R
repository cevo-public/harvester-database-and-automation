#' Find genetically similar sequences from context set to focal set of sequences.
#' By exporting sequences to an alignment and then running the script.
#' @param focal_samples List of sample names of focal sequences.
#' @param tmpdir Directory to create for temporary files.
#' @param overwrite If true, deletes existing tmpdir and its contents.
#' @param clean_tmpdir If true, deletes tmpdir after function complete.
#' @param context_samples List of sample names of context sequences.
#' @param db_connection
#' @return Data frame with columns sample_name and priority with one row per sequence in context set.
get_priority <- function(
  db_connection, focal_samples, context_samples, 
  tmpdir = "tmp/priority",
  overwrite = F,
  clean_tmpdir = T,
  table = "gisaid_sequence",
  sample_name_col = "strain",
  seq_col = "aligned_seq",
  date_col = "date",
  ncovdir = "../ncov",
  python = "python3"
) {
  # Set up tmpdir for temporary output
  if (dir.exists(tmpdir) & overwrite) {
    system(command = paste("rm", tmpdir))
  } else if (dir.exists(tmpdir)) {
    stop(paste(tmpdir, "already exists and overwrite is false."))
  }
  system(command = paste("mkdir -p", tmpdir))
  
  # Write out alignment of focal and context sequences
  export_seqs_as_fasta(
    db_connection = db_connection, 
    sample_names = focal_samples,
    seq_outfile = paste(tmpdir, "focal.fasta", sep = "/"),
    table = table, sample_name_col = sample_name_col, seq_col = seq_col
  )
  export_seqs_as_fasta(
    db_connection = db_connection, 
    sample_names = context_samples,
    seq_outfile = paste(tmpdir, "context.fasta", sep = "/"),
    table = table, sample_name_col = sample_name_col, seq_col = seq_col
  )
  
  # Generate nextstrain-style metadata for the sequences
  export_metadata_as_nextstrain_format(
    db_connection = db_connection,
    metadata_outfile = paste(tmpdir, "metdata.txt", sep = "/"),
    sample_names = c(focal_samples, context_samples),
    table = table, sample_name_col = sample_name_col, date_col = date_col
  )
  
  # Run priority script
  priorities_command <- paste0(python, " ", ncovdir, "/scripts/priorities.py", " --alignment ", tmpdir, "/context.fasta", " --reference ", ncovdir, "/defaults/reference_seq.gb", " --metadata ", tmpdir, "/metadata.txt", " --focal-alignment ", tmpdir, "/focal.fasta", " --output ", tmpdir, "/priorities.txt")
  cat("Running nextstrain priorities script with command:", 
      priorities_command, sep = "\n")
  system(command = priorities_command)
  
  # Return priority results
  priorities <- read.table(
    file = paste0(tmpdir, "/priorities.txt"), sep = "\t") %>%
    rename(strain = V1, "priority" = V2) %>%
    arrange(priority)
  
  # Clean up
  if (clean_tmpdir) {
    system(command = paste("rm", tmpdir))
  }
  return(priorities)
}

#' Find genetically similar sequences from context set to focal set of sequences.
#' By running a modified version of the script that fetches sequenecs directly from the database.
run_nextstrain_priority <- function(
  focal_strains, nonfocal_strains, prefix, outdir, 
  python_path, priorities_script, reference, verbose = F,
  focal_strain_table, context_strain_table, 
  focal_sample_name_col, context_sample_name_col, 
  focal_seq_col, context_seq_col, config_filepath = "database/config.yml"
) {
  n_focal_strains <- length(focal_strains)
  n_nonfocal_strains <- length(nonfocal_strains)
  
  if (n_focal_strains == 0) {
    warning(paste("No focal sequences found for", prefix))
    return(NA)
  } else if (n_nonfocal_strains == 0) {
    warning(paste("No context sequences found for", prefix))
    return(NA)
  } else if (n_nonfocal_strains > 20000) {
    stop(paste("Too many sequences for priorities.py:", 
               n_nonfocal_strains, "non-focal sequences."))
  }
  print(paste("Running priorities.py for", prefix, "with", 
              length(focal_strains), "focal sequences and", 
              length(nonfocal_strains), "non-focal sequences."))
  
  outfile_focal_strains <- paste0(outdir, "/", prefix, "_focal_strains.txt")
  outfile_context_strains <- paste0(outdir, "/", prefix, "_nonfocal_strains.txt")
  write.table(
    x = c("strain", focal_strains), row.names = F, quote = F, col.names = F,
    file = outfile_focal_strains)
  write.table(
    x = c("strain", nonfocal_strains), row.names = F, quote = F, 
    col.names = F, file = outfile_context_strains)

  outfile_priorities <- paste0(outdir, "/", prefix, "_priorities.txt")
  priorities_command <- paste(
    python_path, priorities_script,
    "--focal-strains", outfile_focal_strains,
    "--context-strains", outfile_context_strains,
    "--reference", reference,
    "--outfile", outfile_priorities,
    "--focal-strain-table", focal_strain_table,
    "--context-strain-table", context_strain_table,
    "--focal-sample-name-col", focal_sample_name_col,
    "--context-sample-name-col", context_sample_name_col,
    "--focal-seq-col", focal_seq_col,
    "--context-seq-col", context_seq_col,
    "--config-filepath", config_filepath,
    "--automated")
  if (verbose) {
    print(priorities_command)
  }
  system(command = priorities_command)
  
  priorities <- read.delim(
    file = paste0(outdir, "/", prefix, "_priorities.txt"),
    header = F,
    col.names = c("strain", "priority", "focal_strain"),
    stringsAsFactors = F) %>%
    arrange(desc(priority))
  
  system(command = paste("rm", outfile_focal_strains))
  system(command = paste("rm", outfile_context_strains))
  system(command = paste("rm", outfile_priorities))
  
  return(priorities)
}

#' Export minimal metadata in nextstrain format.
#' @return minimal metadata in nextstrain format. Enables e.g. run of nextstrain 
#' priorities.py and diagnostic.py scripts.
export_metadata_as_nextstrain_format <- function(
  db_connection, sample_names, metadata_outfile,
  table, sample_name_col, date_col, 
  virus = "ncov", region = "dummy_region", date_submitted_delay = 14
) {
  metadata_tbl <- dplyr::tbl(db_connection, table) %>%
    filter(!!sym(sample_name_col) %in% sample_names) %>%
    select(!!sym(sample_name_col), !!sym(date_col)) %>% 
    collect()
  metadata_tbl <- metadata_tbl %>%
    mutate(virus = "ncov", 
           region = "dummy_region", 
           date_submitted = as.Date(!!sym(date_col)) + date_submitted_delay,
           comment = "region and date_submitted have dummy values.") %>%
    rename(name = !!sym(sample_name_col), date = !!sym(date_col))
  write.table(
    x = metadata_tbl, 
    file = metadata_outfile, 
    sep = "\t", row.names = F)
}

#' Get the raw read distribution at a position in a sequence.
get_read_distribution <- function(
  db_connection, sample_name, pos_list, 
  READ_TOPDIR = "/Volumes/shared/covid19-pangolin/backup/working/samples/"
) {
  # Get filepath for basecnt.tsv.gz
  readfile_info <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(sample_name == !! sample_name) %>%
    select(ethid, sample_name, sequencing_batch, seq) %>%
    collect() %>%
    mutate(
      readfile = paste(
        READ_TOPDIR, sample_name, sequencing_batch, "alignments",
        "basecnt.tsv.gz", sep = "/"))
  
  # See what the raw reads are at this position
  read_file <- unlist(readfile_info$readfile)
  print(read_file)
  read_con <- gzfile(read_file, 'rt')
  read_data <- read.table(read_con, skip = 3, header = F)
  close(read_con)
  colnames(read_data) <- c("ref", "pos", "A", "C", "G", "T", "-")
  pos_read_data <- read_data %>% filter(pos %in% c(pos_list))
  
  reference <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(sample_name == "REFERENCE_GENOME") %>%
    select(seq) %>%
    collect
  
  # See what the base calls are
  pos_seq_chars <- toupper(unlist(strsplit(
    x = unlist(readfile_info$seq), split = "")))[pos_list + 1]
  pos_ref_chars <- toupper(unlist(strsplit(
    x = unlist(reference$seq),
    split = "")))[pos_list + 1]
  pos_seq_data <- data.frame(
    pos = pos_list,
    consensus = pos_seq_chars,
    reference = pos_ref_chars)
  pos_data <- merge(x = pos_read_data, y = pos_seq_data, all = T) %>% 
    mutate(pos = as.numeric(as.character(pos)) + 1)
  
  pos_freq_matrix <- t(pos_data[c("A", "C", "T", "G")])
  colnames(pos_freq_matrix) <- pos_data$pos
  return(list(pos_data = pos_data, pos_freq_matrix = pos_freq_matrix))
}

#' Returns whether a sequence has one or more frameshift deletions.
#' @input gaps The 'gaps' output of nextclade.
#' @return Logical, whether the gaps include any of length not divisible by 3.
get_has_frameshift_mutation <- function(gaps) {
  is_frameshift <- F
  if (is.na(gaps) | gaps == "") {
    return(NA)
  }
  split_gaps <- unlist(strsplit(x = gaps, split = ","))
  n_gaps <- length(split_gaps)
  i <- 1
  while (i <= n_gaps & !is_frameshift) {
    gap <- split_gaps[i]
    gap_start_end <- as.numeric(unlist(strsplit(x = gap, split = "-")))
    is_frameshift <- (gap_start_end[2] - gap_start_end[1]) %% 3 != 0
    i <- i + 1
  }
  return(is_frameshift)
}

#' Generates data on frameshift deletions in a sequence.
#' @input gaps The 'gaps' output of nextclade.
#' @return a list of the length of each gap, named by the position of the first gap
get_frameshift_deletions <- function(gaps) {
  frameshifts <- list()
  if (is.na(gaps) | gaps == "") {
    return(frameshifts)
  }
  split_gaps <- unlist(strsplit(x = gaps, split = ","))
  for (gap in split_gaps) {
    gap_start_end <- as.numeric(unlist(strsplit(x = gap, split = "-")))
    gap_length <- gap_start_end[2] - gap_start_end[1]
    if (gap_length %% 3 != 0) {
      frameshifts[[as.character(gap_start_end[1])]] <- gap_length
    }
  }
  return(frameshifts)
}

#' @param seqs A vector of nucleotide sequences with IUAPC characters of the same length.
#' @return The mean value of the pairwise differences between sequences in seqs.
#' Ambiguous bases are considered, e.g. "M" (A or C) and "A" have 0 differences.
get_mean_n_nucleotide_differences <- function(seqs, pairwise = F) {
  seqs_dnabin <- ape::as.DNAbin(strsplit(seqs, split = ""))
  difference_matrix <- phangorn::dist.hamming(x = seqs_dnabin, ratio = F)
  if (pairwise) {
    return(difference_matrix)
  }
  return(mean(difference_matrix))
}

#' Coalesce join
#' Code is adapted from https://alistaire.rbind.io/blog/coalescing-joins/.
#' @param x Dataframe (entries in this one will be prioritized in case of duplicates).
#' @param y Another dataframe.
#' @param by Key column name to join by.
#' @return A dataframe with duplicate columns in x and y coalesced.
coalesce_join <- function(x, y, by = NULL, suffix = c(".x", ".y"), 
                          join = dplyr::left_join, ...) {
  joined <- join(x, y, by = by, suffix = suffix, ...)
  cols <- union(names(x), names(y))  # names of desired output
  
  to_coalesce <- names(joined)[!names(joined) %in% cols]
  suffix_used <- suffix[ifelse(endsWith(to_coalesce, suffix[1]), 1, 2)]
  
  # remove suffixes and deduplicate
  to_coalesce <- unique(substr(
    to_coalesce, 
    1, 
    nchar(to_coalesce) - nchar(suffix_used)))
  
  # cast to factor for dplyr::coalesce
  joined[c(paste0(to_coalesce, suffix[1]), paste0(to_coalesce, suffix[2]))] <- apply(
    X = joined[c(paste0(to_coalesce, suffix[1]), paste0(to_coalesce, suffix[2]))],
    MARGIN = 2, 
    FUN = as.factor)
  
  names(to_coalesce) <- to_coalesce
  coalesced <- purrr::map_dfc(to_coalesce, ~dplyr::coalesce(
    joined[[paste0(.x, suffix[1])]],
    joined[[paste0(.x, suffix[2])]]))
  
  return(dplyr::bind_cols(joined, coalesced)[cols])
}

#' Export sequences from table "consensus_sequence" to fasta file.
#' @param db_connection Connection to the database.
#' @param sample_names List of sample names in column 'sample_name'.
#' @param seq_outfile File to write out to.
#' @param seq_outdir Directory to write out to. If not NULL, writes one file per sample.
#' @param overwrite If true, will overwrite an existing seq_outfile.
#' @param append If true and overwrite is F, will append to an existing seq_outfile.
#' @param header_mapping (Optional) A named list, where values are desired headers and names are sample names.
#' @param fail_incomplete If false, will write out a file even if not all sample_names are found in the database.
#' @param warn_outfile (Optional) File to write out warnings to, like which sequences weren't found.
#' @param mask_from_start (Optional) Number of sites to mask from start of alignment.
#' @param mask_from_end (Optional) Number of sites to mask from end of alignment.
#' @param gzip Boolean indicating whether to compress sequence file using gzip or not.
export_seqs_as_fasta <- function(db_connection, sample_names, 
                                 seq_outfile = NULL, seq_outdir = NULL, 
                                 overwrite = F, append = F, warn_append = T,
                                 header_mapping = NULL, fail_incomplete = T,
                                 warn_outfile = NULL,
                                 table = "consensus_sequence",
                                 sample_name_col = "sample_name",
                                 seq_col = "seq",
                                 mask_from_start = 0,
                                 mask_from_end = 0,
                                 gzip = F
) {
  if (length(sample_names) == 0) {
    warning("No sample names specified for export to fasta. Skipping.")
    return()
  } else if (!is.null(seq_outdir)) {
  } else if (file.exists(seq_outfile) & overwrite) {
    system(command = paste("rm", seq_outfile))
    if (!is.null(warn_outfile) && file.exists(warn_outfile)) {
      system(command = paste("rm", warn_outfile))
    }
  } else if (file.exists(seq_outfile) & append) {
    if (warn_append) {
      warning(paste("Appending to", seq_outfile))
    }
  } else if (file.exists(seq_outfile)) {
    stop(paste(seq_outfile, "already exists and overwrite, append are false."))
  }
  
  if (length(sample_names) == 0 || is.na(sample_names)) {
    stop("No sample_names specified for export to fasta.")
  }
  
  seq_query <- dplyr::tbl(db_connection, table) %>%
    filter(!!sym(sample_name_col) %in% sample_names) %>%
    select(!!sym(sample_name_col), !!sym(seq_col))
  seq_tbl <- seq_query %>% collect()
  
  if (mask_from_start > 0 | mask_from_end > 0) {
    seq_tbl[[seq_col]] <- unlist(lapply(
      X = seq_tbl[[seq_col]],
      FUN = mask_sites_in_seq_string,
      mask_from_start = mask_from_start, mask_from_end = mask_from_end
    ))
  }
  
  if (!all(sample_names %in% seq_tbl[[sample_name_col]])) {
    missing_samples <- sample_names[!(sample_names %in% seq_tbl[[sample_name_col]])]
    if (fail_incomplete) {
      stop(paste("Some samples not found.", missing_samples, sep = "\n"))
    } else {
      warning(paste("Some samples not found.", missing_samples, sep = "\n"))
      if (!is.null(warn_outfile)) {
        warn_outfile_con <- file(warn_outfile, open = "a")
        writeLines(missing_samples, con = warn_outfile_con, sep = "\n")
        close(warn_outfile_con)
      }
      sample_names <- sample_names[!(sample_names %in% missing_samples)]  # remove samples from list for header mapping
    }
  }
  if (!is.null(header_mapping)) {
    if (!all(sample_names %in% names(header_mapping))) {
      stop("header_mapping is missing some sample names.")
    }
    seq_headers <- header_mapping[seq_tbl[[sample_name_col]]]
    seq_tbl[[sample_name_col]] <- seq_headers
  }
  
  seq_tbl <- seq_tbl %>% mutate("header" = paste0(">", !!sym(sample_name_col)))
  if (!is.null(seq_outdir)) {
    for (i in 1:nrow(seq_tbl)) {
      sample_name <- names(seq_headers)[i]
      fp <- paste0(seq_outdir, "/", sample_name, ".fasta")
      seq_outfile_con <- file(fp, open = "a")
      writeLines(
        unlist(seq_tbl[i, c("header", seq_col)]), 
        con = seq_outfile_con, 
        sep = "\n")
      close(seq_outfile_con)
    }
    if (gzip) {
      system(command = paste0("gzip ", seq_outdir, "/*.fasta"))
    }
  } else {
    seq_outfile_con <- file(seq_outfile, open = "a")
    for (i in 1:nrow(seq_tbl)) {
      writeLines(
        unlist(seq_tbl[i, c("header", seq_col)]), 
        con = seq_outfile_con, 
        sep = "\n")
    }
    close(seq_outfile_con)
    if (gzip) {
      system(command = paste0("gzip ", seq_outfile))
    }
  }
}

#' Mask start and end sites of a sequence string.
mask_sites_in_seq_string <- function(seq_string, mask_from_start, mask_from_end, mask_char = "N") {
  start_replacement <- paste0(rep(mask_char, mask_from_start), collapse = "")
  end_replacement <- paste0(rep(mask_char, mask_from_end), collapse = "")
  substr(seq_string, 1, mask_from_start) <- start_replacement
  substr(seq_string, nchar(seq_string) - mask_from_end + 1, nchar(seq_string)) <- end_replacement
  return(seq_string)
}

#' Report what changes will actually be made by update_table.
#' @param table_name The name of the table to update.
#' @param new_table The new table to update the database table from.
#' @param con Connection to the database.
#' @param append_new_rows If false, will only update rows where key_col value already exists & will not add new rows.
#' @param cols_to_update A vector of column names to update.
#' @param key_col Either a character key column name or a list of character key column names.
#' @return nothing, just prints a summary.
summarize_update <- function(
  table_name, new_table, con, append_new_rows, cols_to_update, key_col
) {
  existing_keys <- unlist(dplyr::tbl(con, table_name) %>%
    select(all_of(key_col)) %>%
    collect() %>%
      tidyr::unite(col = "concatenated_keys", all_of(key_col)) %>%
    select(concatenated_keys))
  new_table_keys <- unlist(new_table %>%
    tidyr::unite(col = "concatenated_keys", all_of(key_col)) %>%
    select(concatenated_keys))
  # How many entries will be added?
  if (append_new_rows) {
    n_new_rows <- length(setdiff(new_table_keys, existing_keys))
    print(paste("Adding", n_new_rows, "new entries to table", table_name))
  }
  # How many entries will be updated?
  n_existing_rows <- length(intersect(new_table_keys, existing_keys))
  print(paste("Updating", n_existing_rows, "existing entries in", table_name))
}

#' A helper function for update_table.
#' @param key_col Either a character key column name or a list of character key column names.
#' @return The where clause based on key_col as a string.
get_key_col_sql <- function(key_col) {
  if (length(key_col) == 1) {
    key_col_sql <- paste(
      "WHERE", paste("t", key_col, sep = "."), "=", 
      paste("s", key_col, sep = "."))
  } else {
    key_col_sql <- paste(
      "WHERE", paste("t", key_col[1], sep = "."), "=", 
      paste("s", key_col[1], sep = "."))
    for (i in 2:length(key_col)) {
      key_col_sql_phrase <- paste(
        "AND", paste("t", key_col[i], sep = "."), "=", 
        paste("s", key_col[i], sep = "."))
      key_col_sql <- paste(key_col_sql, key_col_sql_phrase)
    }
  }
  return(key_col_sql)
}

#' Append new rows to a table based on key_col and update values in cols_to_update in all rows.
#' @param table_name The name of the table to update.
#' @param new_table The new table to update the database table from.
#' @param con Connection to the database.
#' @param append_new_rows If false, will only update rows where key_col value already exists & will not add new rows.
#' @param cols_to_update A vector of column names to update.
#' @param key_col Either a character key column name or a list of character key column names.
#' @param table_spec Data frame with table column type specifications.
#' @param close_con If false, leaves connection to database 'con' open so that this function can be called in a loop.
update_table <- function(
  table_name, new_table, con, append_new_rows = T, cols_to_update, key_col, 
  table_spec, close_con = T, run_summarize_update = T, verbose = F
) {
  if (run_summarize_update) {
    summarize_update(
      table_name, new_table, con, append_new_rows, cols_to_update, key_col)
  }
  
  # create staging table
  staging_table_name <- paste0(table_name, "_staging")
  DBI::dbBegin(con)
  if (DBI::dbExistsTable(con, staging_table_name)) {
    DBI::dbRemoveTable(con, staging_table_name)
  }
  field_types <- table_spec$type
  names(field_types) <- table_spec$name
  field_types <- field_types[names(field_types) %in% colnames(new_table)]  # only provide field types for columns to be imported to staging table
  if (verbose) {
    print(paste("Writing staging table", staging_table_name, "to", con))
    print(paste("Field types are:", field_types))
  }
  DBI::dbWriteTable(
    con, staging_table_name, new_table, field.types = field_types,
    row.names=FALSE)

  # Append columns from staging for rows that only exist in staging
  key_col_sql <- get_key_col_sql(key_col)
  if (append_new_rows) {
    append_sql <- paste(
      "INSERT INTO", table_name, "(", paste0(c(key_col, cols_to_update), collapse = ", "), ")",
      "SELECT", paste0(c(key_col, paste0("s.", cols_to_update)), collapse = ", "), "FROM", staging_table_name, "s",
      "WHERE NOT EXISTS",
      "(SELECT 1 FROM", table_name, "t",
      key_col_sql,
      ")")
    res <- DBI::dbSendStatement(con, append_sql)
    DBI::dbClearResult(res)
  }

  # Update values in table based on values in staging table
  update_equalities <- c()
  for (col in cols_to_update) {
    update_equalities <- c(update_equalities, paste0(col, " = ", paste("s.", col, sep = "")))
  }
  update_sql <- paste(
    "UPDATE", table_name, "t",
    "SET", paste0(update_equalities, collapse = ", "),
    "FROM", staging_table_name, "s",
    key_col_sql)
  res <- DBI::dbSendStatement(con, update_sql)
  DBI::dbClearResult(res)

  DBI::dbRemoveTable(con, staging_table_name)
  if (close_con) {
    DBI::dbCommit(con)
  }
}

#' Parse ethid from strain names given by GISAID.
#' @param gisaid_strain The virus name on GISAID. Format should be 'Switzerland/<Canton code>-ETHZ-<ethid>/<Year>'
#' @return The ethid (an integer)
get_ethid_from_gisaid_strain <- function(gisaid_strain) {
  if (!grepl(x = gisaid_strain, pattern = "ETHZ")) {
    warning(paste(gisaid_strain, "not recognized as being from ETHZ returning NA for ethid"))
    return(NA)
  }
  ethid <- strsplit(x = gisaid_strain, split = "-")[[1]][length(strsplit(x = gisaid_strain, split = "-")[[1]])]  # take everything after last "-"
  ethid <- strsplit(x = ethid, split = "/")[[1]][1]  # take everything before first "/"
  ethid <- gsub(x = ethid, pattern = "plus", replacement = "")  # remove suffix "plus" if present
  ethid <- as.numeric(ethid)
  return(ethid)
}

#' Parse ethid from sample names given by the sequencing center (how sequencing results are labeled).
#' The ETHID is assumed to be the first 6 or 8 digits in the sample name, 
#' so long as this is followed by an underscore or end of sample name and is 
#' present in either the ethid (6-digits) or sample_number (8-digits) column in 
#' the viollier_test table.
#' @param sample_name The name given to the sample by the sequencing center.
#' @param db_connection
#' @return The ethid (a 6-digit integer)
get_ethid_from_sample_name <- function(sample_name, db_connection) {
  is_ethid_format <- grepl(
    x = sample_name,
    pattern = "^[[:digit:]]{6}(_.*|$)")
  is_sample_number_format <- grepl(
    x = sample_name,
    pattern = "^[[:digit:]]{8}(_.*|$)")
  if (is_ethid_format) {  # Check if ETHID is in the viollier_test table
    ethid <- unlist(strsplit(sample_name, split = "_"))[1]
    in_vt_once <- nrow(
      dplyr::tbl(db_connection, "viollier_test") %>% 
        filter(ethid == !! ethid) %>% 
        collect()) == 1
    if (in_vt_once) {
      return(ethid)
    } else {
      warning(paste(ethid, "found 0 or > 1 times in the Viollier test table column 'ethid'. Invalid ETHID."))
      return(NA)
    }
  } else if (is_sample_number_format) {  # Check if sample number is in the viollier_test table
    ethid <- unlist(strsplit(sample_name, split = "_"))[1]
    in_vt_once <- nrow(
      dplyr::tbl(db_connection, "viollier_test") %>% 
        filter(sample_number == !! ethid) %>% 
        collect()) == 1
    if (in_vt_once) {
      return(ethid)
    } else {
      warning(paste(ethid, "found 0 or > 1 times in the Viollier test table column 'sample_number'. Invalid ETHID."))
      return(NA)
    }
  } else {  # Control sample, sample from ZRH, EAWAG sample, other non-standard sample
    warning(paste("ethid not found in sample name:", sample_name, "\n", sep = " "))
    return(NA)
  }
}

#' Enforce table specifications given in init.sql file
#' @param table The data table to be formatted.
#' @param table_name The table name in the sql_specification file.
#' @param db_connection Connection to the database.
#' @param type_prefix Normally the script tries to guess what custom types for this table would be named. But you can also specify a prefix here.
#' @return A data table coerced to the types specified in the database schema.
enforce_sql_spec <- function(
  table, table_name, db_connection, type_prefix = NULL, n_cores = 1, verbose = F
) {
  table_spec <- parse_table_specification(table_name = table_name, db_connection = db_connection)
  col_spec <- parse_column_specification(table_spec = table_spec, db_connection = db_connection)
  for (colname in colnames(table)) {
    if (verbose) {
      print(paste("Checking for special column type for column", colname))
    }
    col_spec_names <- c(
      colname, 
      paste(unique(table_name, type_prefix), colname, sep = "_"), 
      paste(unique(table_name, type_prefix), colname, "type", sep = "_"))
    col_spec_name <- col_spec_names[which(col_spec_names %in% names(col_spec))]
    if (length(col_spec_name) > 0) {
      if (verbose) {
        print(paste("Found special column type", col_spec_name, "for column", colname))
      }
      unique_vals <- unique(table[[colname]])
      unknown_vals <- unique_vals[!(unique_vals %in% col_spec[[col_spec_name]])]
      if (length(unknown_vals) > 0) {
        stop(cat("Unkown values ", paste0(unknown_vals, collapse = ", "), " in column ", colname))
      }
    } else {
      if (verbose) {
        print(paste("Enforcing specified type for column", colname))
      }
      table[[colname]] <- enforce_column_type(
        colname = colname,
        values = table[[colname]],
        type = table_spec[table_spec$name == colname, "type"],
        n_cores = n_cores)
    }
    if (table_spec[table_spec$name == colname, "unique"]) {
      if (anyDuplicated(table[[colname]])) {
        stop(paste(colname, "specified to be unique but is not."))
      }
    }
  }
  return(table)
}

#' Parse table specifications given in init.sql file
#' @param table_name The table name in the sql_specification file.
#' @param db_connection Connection to the database.
#' @return A data frame with columns 'name' for column name, 'type' for column type, and 'unique' for whether the column should be unique or not
parse_table_specification <- function(table_name, db_connection) {
  # Get column types
  sql <- paste0("SELECT column_name, udt_name FROM
  INFORMATION_SCHEMA.COLUMNS WHERE table_name = '", table_name, "';")
  res <- DBI::dbSendQuery(conn = db_connection, statement = sql)
  col_types <- DBI::dbFetch(res)
  DBI::dbClearResult(res)

  # Get column constraints
  sql <- paste0("SELECT Col.Column_Name FROM
  INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab,
  INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col
  WHERE Col.Constraint_Name = Tab.Constraint_Name
  AND Col.Table_Name = Tab.Table_Name
  AND (Constraint_Type = 'PRIMARY KEY' OR Constraint_Type = 'UNIQUE')
  AND Col.Table_Name = '", table_name, "';")
  res <- DBI::dbSendQuery(conn = db_connection, statement = sql)
  col_constraints <- DBI::dbFetch(res)
  DBI::dbClearResult(res)

  table_spec <- col_types %>%
    rename(name = column_name, type = udt_name) %>%
    mutate(unique = ifelse(
      test = name %in% col_constraints$column_name,
      yes = T, no = F))
  return(table_spec)
}

#' Parse column specifications given in init.sql file
#' @param table_spec The table specification. Output of parse_table_specification function.
#' @param table_name The table name in the sql_specification file. Must provide either table_spec or table_name.
#' @param db_connection Connection to the database.
#' @return A named list, where names are column names and values are values the column can take.
parse_column_specification <- function(table_spec = NULL, table_name = NULL, db_connection) {
  if (is.null(table_spec)) {
    table_spec <- parse_table_specification(
      table_name = table_name, db_connection = db_connection)
  }
  sql <- "SELECT pg_type.typname, pg_enum.enumlabel FROM
  pg_type JOIN
  pg_enum ON
  pg_enum.enumtypid = pg_type.oid;"
  res <- DBI::dbSendQuery(conn = db_connection, statement = sql)
  udt_all <- DBI::dbFetch(res)
  DBI::dbClearResult(res)

  udt_tbl <- udt_all %>% filter(typname %in% table_spec$type)
  col_spec <- split(udt_tbl$enumlabel, udt_tbl$typname)
  return(col_spec)
}

#' Enforce correct data type for a column
#' @param type The column type as a character value. One of 'text', 'date', 'integer', 'float4', 'float8', 'boolean'.
#' @return Values transformed to correct type.
enforce_column_type <- function(colname, values, type, n_cores = 1) {
  if (type == "text") {
    coerceion_function <- as.character
  } else if (type == "int4") {
    coerceion_function <- as.integer
  } else if (type == "date") {
    return(as.character(unlist(parallel::mclapply(X = values, FUN = standardize_date, mc.cores =  n_cores))))
  } else if (type == "bool") {
    coerceion_function <- as.logical
  } else if (type == "float4" | type == "float8") {
    coerceion_function <- as.numeric
  } else {
    stop(paste("Unrecognized type", type))
  }
  coerced_values <- tryCatch(
    coerceion_function(values),
    error = function(cond) {
      message(paste("Can't coerce column", colname, "to type", type))
      message(cond)
      stop()
    },
    warning = function(cond) {
      message(paste("Can't coerce column", colname, "to type", type))
      message(cond)
      stop()
    })
  return(coerced_values)
}

#' Convert input data to date data type. Errors if cannot be interpreted as a date.
#' Also check if the resulting date falls between Feb. 24 2020 and current date.
#' @return date
standardize_date <- function(date) {
  formatted_date <- tryCatch(
    expr = lubridate::as_date(date),
    error = function(cond) {
      message(cond)
      stop(paste("Date", date, "is in unrecognized format."))
    }
  )
  if (!is.na(formatted_date)) {
    range_check <- formatted_date >= as.Date("2020-02-24") & formatted_date <= as.Date(Sys.Date())
    if (!range_check) {
      warning(paste("Date", date, "was interpreted as", formatted_date, "which is out of reasonable range. Replacing value with NA"))
      return(NA)
    }
  }
  return(as.character(formatted_date))
}

#' Get precedence order of data given multiple data files
#' @return A named list, where names are unique filenames and values are precendence order of data
getfilename_priority_bag_meldeformular <- function(filenames) {
  filename_priorities <- data.frame(
    filename = unique(filenames),
    filedate = unlist(lapply(
      X = unique(filenames),
      FUN = get_bag_meldeformular_file_date))) %>%
    arrange(desc(filedate)) %>%
    mutate(priority = 1:n())
  priorities <- filename_priorities$priority
  names(priorities) <- filename_priorities$filename
  return(priorities)
}

#' Get file date from bag meldeformular filename
#' @return Date as character type
get_bag_meldeformular_file_date <- function(bag_filename) {
  file_date <- gsub(x = bag_filename, pattern = "\\/", replacement = "")
  file_date <- strsplit(file_date, split = "_")[[1]][1]
  return(file_date)
}

#' This function opens a connection to the database. The password has to be entered through a prompt.
#' @param config_file The path to the config file if it is not in the working directory.
#' @return (DBI::DBIConnection)
open_database_connection <- function (
  db_instance = Sys.getenv("DB_INSTANCE", "local"), password_method = "config",
  config_file = NA
) {
  if (is.na(config_file)) {
    connection_data <- config::get("database")[[db_instance]]
  } else {
    connection_data <- config::get("database", file = config_file)[[db_instance]]
  }

  if (password_method == "askpass") {
    password <- askpass::askpass(paste0("Please enter the password for user \"", connection_data$username, "\":"))
  } else if (password_method == "readline") {
    cat(paste0("Please enter the password for user \"", connection_data$username, "\" on host \"",
               connection_data$host, "\":\n"))
    password <- readLines(file("stdin"), n = 1L)
  } else if (password_method == "config") {
    password <- connection_data$password
  }
  db_connection <- DBI::dbConnect(
    RPostgres::Postgres(),
    host = connection_data$host,
    port = connection_data$port,
    user = connection_data$username,
    password = password,
    dbname = connection_data$dbname
  )
  return(db_connection)
}

#' Check if iso code is in country table. Unknown codes get overwritten 
#' @param iso_country ISO country code.
#' @param db_connection
#' @return iso_country if code is in country table, 'XXX' otherwise
get_standardized_iso_country <- function(iso_country, db_connection) {
  known_codes <- unlist(dplyr::tbl(db_connection, "country") %>%
    select(iso_code) %>% collect())
  unknown_codes <- iso_country[!(iso_country %in% known_codes)]
  if (length(unknown_codes) > 0) {
    warning(
      "These codes are not in the country table and will be overwritten with 'XXX': ",
      paste0(unique(unknown_codes), collapse = ", ")
    )
  }
  iso_country[!(iso_country %in% known_codes)] <- 'XXX'
  return(iso_country)
}

#' Convert iso codes to English language country names
iso_code_to_country_name <- function(iso_code) {
  return(countrycode::countrycode(
    sourcevar = iso_code,
    origin = "iso3c",
    destination = "country.name",
    custom_match = c("KOS" = "Kosovo", 'OTHER' = "Other", 'UNKNOWN' = "Unknown", 'XXX' = 'Unknown')))
}

#' Guess ISO country code from English or German language country names
#' @param language 'english' if country is in English, 'german' if in German
country_name_to_iso_code <- function(country, language = "english") {
  if (language == "english") {
    codes <- countrycode::countrycode(
      sourcevar = country, 
      origin = "country.name", 
      destination = "iso3c",
      custom_match = c("Kosovo" = "KOS"))
  } else if (language == "german") {
    codes <- countrycode::countrycode(
      sourcevar = country, 
      origin = "country.name.de", 
      destination = "iso3c",
      custom_match = c("Kosovo" = "KOS"))
  } else {
    stop(paste("Cannot translate country names from specified language:", language))
  }
  return(codes)
}

#' Given a list of sequencing batches, check the list of sequences in the database
#' against the list of samples in V-pipe's sampleset/samples.<batch>.tsv file. 
#' @param batches List of batch names, e.g. c('20210326_H5YL5DRXY')
#' @param samples_in_database List of sample names from database to check against.
#' @param sampleset_dir Directory with V-pipe's sampleset/samples.<batch>.tsv files.
#' @return Logical vector indicating whether each of batches is complete.
check_all_seqs_imported <- function(
  batches, 
  samples_in_database,
  sampleset_dir = "/Volumes/shared/covid19-pangolin/backup/sampleset",
  verbose = T) {
  batches_are_complete <- rep(F, length(batches))
  for (i in 1:length(batches)) {
    batch <- batches[i]
    sampleset_file <- paste0(sampleset_dir, "/samples.", batch, ".tsv")
    if (!file.exists(sampleset_file)) {
      print(log.warn(
        msg = paste0("File ", sampleset_file, " does not exist! Cannot check whether all samples in database."),
        fcn = paste0("utility.R", "::", "check_all_seqs_imported")))
      batches_are_complete[i] <- F
    } else {
      sampleset <- read.delim(file = sampleset_file, header = F)
      samples_not_in_database <- sampleset$V1[!(sampleset$V1 %in% samples_in_database)]
      if (length(samples_not_in_database) > 0) {
        if (verbose) {
          warning("Some samples from batch not found in the database!\n",
                  paste0(samples_not_in_database, collapse = "\n"))
        }
      } else {
        batches_are_complete[i] <- T
      }
    }
  }
  names(batches_are_complete) <- batches
  return(batches_are_complete)
}
