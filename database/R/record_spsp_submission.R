
# Load requirements
source("R/utility.R")
source("R/logger.R")
require(dplyr)
require(yaml)
require(argparse)

#' Import sample_name that will be released to table "sequence_identifier".
main <- function(args) {
  if (!has_sent_files(args)) {
    print(log.info(
      msg = "No files sent to SPSP found. Not updating table sequence_identifier.",
      fcn = paste0(args$script_name, "::", "main")))
    return()
  }
  db_connection <- connect_to_db(args)
  metadata <- load_metadata(args)
  update_sequence_identifier(db_connection, metadata, args)
}

#' Return boolean for whether there's a file found in the '<outdir>/sent' directory
has_sent_files <- function(args) {
  sent_dir <- paste(args$outdir, "sent", sep = "/")
  sent_files <- list.files(sent_dir)
  if(length(sent_files) > 0) {
    return(T)
  } else {
    return(F)
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

#' @return submitted sequence metadata.
load_metadata <- function(args) {
  submission_dir <- paste(args$outdir, "for_submission/viruses", Sys.Date(), sep = "/")
  metadata_file <- list.files(path = submission_dir, pattern = "metadata-file.*\\.tsv")
  if (length(metadata_file) != 1) {
    print(log.error(
      msg = paste("Cannot find sinlge expected submitted metadata file in:", submission_dir),
      fcn = paste0(args$script_name, "::", "load_metadata")))
    stop()
  }
  metadata <- read.delim(
    file = paste(submission_dir, metadata_file, sep = "/"), 
    row.names = "reporting_lab_order_id")
  print(log.info(
    msg = paste("Found metadata file", metadata_file, "with", nrow(metadata), "entries."),
    fcn = paste0(args$script_name, "::", "load_metadata")))
  return(metadata)
}

update_sequence_identifier <- function(db_connection, metadata, args) {
  print(log.info(
    msg = "Updating sequence_identifier table with released samples.",
    fcn = paste0(args$script_name, "::", "update_sequence_identifier")))
  table_spec <- parse_table_specification(
    table_name = "sequence_identifier",
    db_connection = db_connection)
  prev_table <- dplyr::tbl(db_connection, "sequence_identifier") %>%
    collect()
  newly_uploaded <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(sample_name %in% !! rownames(metadata)) %>%
    select(ethid, sample_name) %>%
    collect()
  joined_table <- coalesce_join(
    x = newly_uploaded %>%
      mutate(spsp_uploaded_at = Sys.Date()),
    y = prev_table,
    by = "ethid")

  update_table(
    table_name = "sequence_identifier",
    new_table = joined_table,
    con = db_connection,
    append_new_rows = T,
    cols_to_update = c("sample_name", "spsp_uploaded_at"),
    key_col = "ethid",
    table_spec = table_spec)
}

# Production arguments
parser <- argparse::ArgumentParser()
parser$add_argument("--config", type = "character", help = "Path to spsp-config.yml.", default = "spsp-config.yml")
parser$add_argument("--outdir", type = "character", help = "Path to output files for submission.", default = paste("/mnt/pangolin/consensus_data_for_release/spsp_submission", Sys.Date(), sep = "/"))
args <- parser$parse_args()
args[["script_name"]] <- "record_spsp_submission.R"

# Test arguments
# args <- list()
# args[["config"]] <- "spsp-config.yml"
# args[["outdir"]] <- "~/Downloads/test_outdir"
# args[["outdir"]] <- "/Volumes/covid19-pangolin/pangolin/consensus_data_for_release/spsp_test"
# args[["script_name"]] <- "record_spsp_submission.R"

# Run program
main(args = args)
