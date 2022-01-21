# Title     : Manually import team w metadata
# Objective : This script imports metadata (from the same file Andreas sends to FGCZ) for labor team w samples for which we already recieved sequences. It will also assign ETHIDs to the samples.
# Created by: nadeaus
# Created on: 18.08.21

require(dplyr)
source("R/utility.R")
db_connection <- open_database_connection("server")

# Load team w metadata from the files they send to FGCZ
metadata_dir <- ""

#' Read in team w metadata.
load_metadata <- function(file) {
  # Check if metadata is in original format (plate name in first row) or revised format (has PlateName column)
  file_con <- file(file)
  first_line <- readLines(con = file_con, n = 1)
  close(file_con)
  if (grepl(pattern = "PlateName", x = first_line)) {
    metadata <- load_revised_metadata(file)
  } else {
    metadata <- load_original_metadata(file)
  }
  return(metadata)
}

#' Read in team w metadata in original format (plate name in first row).
load_original_metadata <- function(file) {
  metadata <- read.table(file = file, sep = ";", skip = 1, header = T)
  file_con <- file(file)
  plate_i <- readLines(con = file_con, n = 1)
  close(file_con)
  plate_i_parsed <- gsub(pattern = "Platte |;", x = plate_i, replacement = "")
  metadata <- metadata %>%
    mutate(PlateName = plate_i_parsed) %>%
    filter(LabOrderId != "NC") %>%
    mutate(sample_number = gsub(pattern = "Y", replacement = "", x = LabOrderId))
  if ("CollectionDate" %in% colnames(metadata)) {
    metadata <- metadata %>% mutate(order_date = as.Date(CollectionDate, format = "%d.%m.%y"))
  } else {  # If no collection date column, take collection date based on plate name
    metadata <- metadata %>%
      mutate(
        CollectionDate = NA,
        order_date_str = stringr::str_extract(string = PlateName, pattern = "^21[[:digit:]]{4}"),
        order_date = as.Date(order_date_str, format = "%y%m%d") - 4) %>%  # plate name is Mon extraction date, includes samples from prev Tues - Sat so date samples to prev Thurs
      select(-order_date_str)
  }
  return(metadata)
}

#' Read in team w metadata in revised format (has PlateName column).
load_revised_metadata <- function(file) {
  metadata <- read.table(file = file, sep = ";", header = T)
  metadata <- metadata %>%
    filter(LabOrderId != "NC") %>%
    mutate(sample_number = gsub(pattern = "Y", replacement = "", x = LabOrderId),
           order_date = as.Date(CollectionDate, format = "%d.%m.%y"))
  return(metadata)
}

is_first <- T
for (file in list.files(path = metadata_dir, full.names = T)) {
  print(paste("Reading metadata from", file))
  if (is_first) {
    metadata <- load_metadata(file)
    is_first <- F
  } else {
    metadata <- rbind(metadata, load_metadata(file))
  }
}

# Check there are 93 samples per plate (this seems to be the standard for team w - plates with 93 named samples and 1 "NC")
n_samples_per_plate <- metadata %>% group_by(PlateName) %>% summarize(n_samples = n(), .groups = "drop")
if (any(n_samples_per_plate$n_samples != 93)) {
  bad_plates <- n_samples_per_plate %>% filter(n_samples != 93)
  print(bad_plates)
  stop("Didn't find 93 metadata entries for some plate(s).")
}

# Match the metadata to sequences based on the metadata "LabOrderId" matching part of the sequence "sample_name"
sample_names <- dplyr::tbl(db_connection, "consensus_sequence") %>%
  select(sample_name, sequencing_batch) %>%
  collect()

team_w_plate_name_lab_order_id_pattern <- "Y22[[:digit:]]{7}"
#team_w_plate_name_lab_order_id_pattern <- "Y21[[:digit:]]{7}_2021[[:digit:]]{4}_21[[:digit:]]{8}_" 
#OLD Ex: 2108028604_Y210959950 from 2108028604_Y210959950_32033_S87
#NEW Ex: Y211178764_20210920_2109208604 from Y211178764_20210920_2109208604_S17
#NEW Ex 12Dec: Y211643907

sample_names_parsed <- sample_names %>%
  mutate(
    team_w_plate_name_lab_order_id = stringr::str_extract(string = sample_name, pattern = team_w_plate_name_lab_order_id_pattern)) %>%
  tidyr::separate(
    col = team_w_plate_name_lab_order_id,
    sep = "_",
    into = c("LabOrderId", "Unknown field", "PlateName"),
    extra = "drop",
    fill = "right") %>%
  filter(!is.na(LabOrderId)) %>%
#  select(sample_name, sequencing_batch, PlateName, LabOrderId)
  select(sample_name, sequencing_batch, LabOrderId)

#metadata_to_sample_name <- base::merge(
#  x = metadata,
#  y = sample_names_parsed,
#  by = c("PlateName", "LabOrderId"),
#  all.x = T,
#  all.y = F)

metadata_to_sample_name <- base::merge(
  x = metadata,
  y = sample_names_parsed,
  by = c("LabOrderId"),
  all.x = T,
  all.y = F)

# Check that whole plates are matched to in the sequencing data
n_sequences_per_plate <- metadata_to_sample_name %>%
  group_by(PlateName) %>%
  summarize(n_sequences_matched = sum(!is.na(sample_name)), .groups = "drop") %>%
  collect()

if (any(!(n_sequences_per_plate$n_sequences_matched %in% c(0, 93)))) {
  bad_plates <- n_sequences_per_plate %>% filter(!(n_sequences_matched %in% c(0, 93)))
  print(bad_plates)
  suspicious_samples <- metadata_to_sample_name %>% filter(PlateName %in% bad_plates$PlateName)
  print(suspicious_samples)
  stop(paste("Didn't find 93 sequences for some plate(s) in the database table 'consensus_sequence'.",
             "This could be because of all-N samples from FGCZ that get imported before the rest of the batch processes in V-pipe.",
             "Make sure this is the case by checking `number_n` for these samples in the `consensus_sequence` table."))
}

# Clean the metadata for the sequences
key_col = "sample_name"
cols_to_update <- c("order_date", "zip_code", "covv_orig_lab", "covv_orig_lab_addr", "sample_number")

metadata_to_import <- metadata_to_sample_name %>%
  filter(!is.na(sample_name)) %>%
  rename(zip_code = PLZ) %>%
  mutate(
    covv_orig_lab = "labor team w AG",
    covv_orig_lab_addr = "Blumeneggstrasse 55, 9403 Goldach") %>%
  select(all_of(c(key_col, cols_to_update)))

# Import the metadata for the sequences to non_viollier_test
table_spec <- parse_table_specification(table_name = "non_viollier_test", db_connection = db_connection)
update_table(
  table_name = "non_viollier_test",
  new_table = metadata_to_import,
  con = db_connection,
  cols_to_update = cols_to_update,
  key_col = key_col,
  table_spec = table_spec)

# Assign team w samples with metadata ETHIDs
consensus_sequence_query <- paste(
  "select cs.sample_name, ethid, sequencing_batch, number_n",
  "from consensus_sequence cs",
  "right join non_viollier_test nvt on cs.sample_name = nvt.sample_name",
  "where nvt.covv_orig_lab = 'labor team w AG'",
  "and ethid is null;")

consensus_sequence_exerpt <- DBI::dbGetQuery(conn = db_connection, statement = consensus_sequence_query)

do_update <- F
if (nrow(consensus_sequence_exerpt) %% 93 != 0) {
  warning(paste("The number of sequences from labor team w AG in consensus_sequence without an ethid is not a multiple of 93!",
                "Only assigning ethids to sequences from fully imported plates."))
  consensus_sequence_exerpt <- consensus_sequence_exerpt %>%
    group_by(sequencing_batch) %>%
    mutate(n_sequences = n())
  print("Not assigning ETHIDs to these sequences:")
  print(consensus_sequence_exerpt %>% filter(n_sequences != 93))
  consensus_sequence_exerpt <- consensus_sequence_exerpt %>% filter(n_sequences == 93)
}
if (nrow(consensus_sequence_exerpt) == 0) {
  print("No labor team w AG sequences identified in non_viollier_test that are missing ETHIDs.")
} else {
  print(paste("Generating ETHIDs for labor team w AG sequences from", nrow(consensus_sequence_exerpt) / 93, "plates."))

  ethids <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(ethid <= 999999) %>%
    select(ethid) %>%
    collect()
  start_ethid <- max(max(ethids) + 1, 670000)  # labor team w ethids start at 670000 but I want to be sure I don't overwrite any if we run this script again

  consensus_sequence_to_import <- consensus_sequence_exerpt %>%
    arrange(sequencing_batch, sample_name) %>%
    mutate(ethid = start_ethid:(start_ethid + nrow(consensus_sequence_exerpt) - 1))
  do_update <- T
}

if (do_update) {
  table_spec <- parse_table_specification(table_name = "consensus_sequence", db_connection = db_connection)
  update_table(
    table_name = "consensus_sequence",
    new_table = consensus_sequence_to_import,
    con = db_connection,
    cols_to_update = "ethid",
    key_col = c("sample_name", "sequencing_batch"),
    table_spec = table_spec)
}

