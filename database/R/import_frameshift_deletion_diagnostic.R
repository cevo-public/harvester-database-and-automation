suppressMessages(suppressWarnings(require(argparse)))
suppressMessages(suppressWarnings(require(RPostgres)))
suppressMessages(suppressWarnings(require(tidyverse)))
source("database/R/utility.R")

#' Update table "frameshift_deletion_diagnostic" with deletion diagnostic information.
#' @param samples_dir Path to V-pipe samples directories, which contain the deletion diagnostic csv files
#' @param sequencing_batches A list of batches to import files for. If null (default) looks for files for all samples already in the database.
#' e.g. "/cluster/project/pangolin/working/samples"
import_deletion_diagnostics <- function(
  samples_dir, db_connection, append_only = F, sequencing_batches = NULL
) {
  if (!(is.null(sequencing_batches))) {
    samples_to_import <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      filter(sequencing_batch %in% !! sequencing_batches) %>%
      select(sample_name, sequencing_batch) %>%
      collect()
  } else {
    samples_to_import <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      select(sample_name, sequencing_batch) %>%
      collect()
  }
  if (append_only) {
    already_imported <- unique(dplyr::tbl(db_connection, "frameshift_deletion_diagnostic") %>%
      select(sample_name) %>%
      collect())
    print(paste(nrow(already_imported), "out of", nrow(samples_to_import), "already imported. Skipping these."))
    samples_to_import <- samples_to_import %>%
      filter(!(sample_name %in% !! already_imported$sample_name))
  }
  n_samples <- nrow(samples_to_import)
  print(paste("Looking for frameshift deletion diagnostic info for", n_samples, "samples."))
  for (batch_i in unique(samples_to_import$sequencing_batch)) {
    samples_to_import_i <- samples_to_import %>% filter(sequencing_batch == batch_i)
    is_first <- T
    for (sample_name in samples_to_import_i$sample_name) {
      dels_file <- paste(
        samples_dir, sample_name, batch_i, 
        "references/frameshift_deletions_check.tsv", sep = "/")
      if (file.exists(dels_file)) {
        dels_i <- read.table(file = dels_file, header = T, sep = "\t", quote = "", row.names = 1)
        dels_i <- clean_deletion_diagnostics(dels_i) %>% mutate(sample_name = sample_name)
        if (is_first) {
          is_first <- F
          dels <- dels_i
        } else {
          dels <- rbind(dels, dels_i)
        }
      } else {
        print(paste("No file exists where expected:", dels_file))
      }
    }
    if (nrow(dels) > 0) {
      update_database(db_connection = db_connection, dels = dels)
      print(paste("Finished deletion diagnostic import for batch", batch_i))
    } else {
      print(paste("No or only empty deletion diagnostic files in batch", batch_i))
    }
  }
}

#' Clean deletion diagnostics table to match database table "frameshift_deletion_diagnostic" format
clean_deletion_diagnostics <- function(dels) {
  dels_clean <- dels %>%
    rename(
      indel_type = INDEL,
      indel_position = indel_position_english) %>%
    mutate(
      ref_base = gsub(x = ref_base, pattern = "b'|'", replacement = ""),
      freq_del_rev = as.double(freq_del_rev),
      freq_del_fwd = as.double(freq_del_fwd)) %>%
    select(-c(ref_id, cons_id))
  return(dels_clean)
}

#' Add new rows to deletion data table in the database
update_database <- function(db_connection, dels) {
  key_col = c("sample_name", "start_position")
  table_spec <- parse_table_specification(
    table_name = "frameshift_deletion_diagnostic", db_connection = db_connection)
  cols_to_update <- colnames(dels)[!(colnames(dels) %in% key_col)]

  update_table(
    table_name = "frameshift_deletion_diagnostic",
    new_table = dels,
    con = db_connection,
    append_new_rows = T,
    cols_to_update = cols_to_update,
    key_col = key_col,
    table_spec = table_spec,
    run_summarize_update = T)
}

parser <- argparse::ArgumentParser()
parser$add_argument("--samplesdir", type="character")
parser$add_argument("--dbhost", type="character")
parser$add_argument("--dbport", type="double", default=5432)
parser$add_argument("--dbuser", type="character")
parser$add_argument("--dbpassword", type="character")
parser$add_argument("--dbname", type="character", default="sars_cov_2")
parser$add_argument("--batch", type="character", default = NULL)

args <- parser$parse_args()

samples_dir <- args$samplesdir
db_host <- args$dbhost
db_port <- args$dbport
db_user <- args$dbuser
db_password <- args$dbpassword
db_name <- args$dbname
sequencing_batch <- args$batch

print("Connecting to database.")
db_connection <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = db_host,
  port = db_port,
  user = db_user,
  password = db_password,
  dbname = db_name)

print("Importing frameshift diagnostics.")
dels_with_diagnostic <- import_deletion_diagnostics(samples_dir, db_connection, sequencing_batches = sequencing_batch)
