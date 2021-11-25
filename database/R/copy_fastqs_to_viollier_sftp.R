# Title     : Copy fastqs to droppoint that's synced with Viollier SFTP.
# Objective : Temporary means of transfering sequence requests.
# Created on: 29.06.21
# Usage     : Connect to D-BSSE server pangolin/ group share, replace sample number list with relevant samples, run script

source("R/utility.R")
source("R/secrets.R")
require(dplyr)

PANGODIR <- "/Volumes/covid19-pangolin"
SAMPLE_NUMBERS <- c(
  # Paste sample numbers here, e.g. '#########'
)

if (!dir.exists(PANGODIR)) {
  stop("Must be connected to:", PANGODIR)
}

# Ask user whether to push files to Viollier SFTP server after copying them all to D-BSSE outbox
continue <- 'x'
while (!(continue %in% c('y', 'n'))) {
  sync_prompt <- paste(
    "Initiate synchronization of D-BSSE outbox with Viollier SFTP server? (y/n)",
    "This will only work if Ivan added your public key ~/.ssh/id_rsa_pangolin.",
    sep = "\n")
  continue <- readline(prompt = sync_prompt)
}

# Look up sample info (including sample name) for the provided sample numbers
db_connection <- open_database_connection("server")
sample_info_with_metadata <- dplyr::tbl(db_connection, "viollier_test") %>% select(ethid, sample_number) %>%
  filter(sample_number %in% !! SAMPLE_NUMBERS) %>%
  left_join(dplyr::tbl(db_connection, "consensus_sequence") %>% select(ethid, sample_name), by = "ethid") %>%
  left_join(dplyr::tbl(db_connection, "viollier_test__viollier_plate") %>% select(sample_number, viollier_plate_name), by = "sample_number") %>%
  left_join(dplyr::tbl(db_connection, "viollier_plate") %>% select(sequencing_center, left_viollier_date, viollier_plate_name), by = "viollier_plate_name") %>%
  collect()

sample_info <- merge(
  sample_info_with_metadata,
  data.frame(sample_number = SAMPLE_NUMBERS, sample_status = "no metadata recieved: also no sample recieved?"),  # placeholder status
  all = T, by = "sample_number")

# Find not-transferred files and copy them to SFTP server
already_transferred_dirs <- list.dirs(
  path = paste(PANGODIR, "backup/sftp-viollier/raw_othercenters", sep = "/"),
  recursive = T,
  full.names = F)

base_dest_dir <- paste(PANGODIR, "backup/sftp-viollier/raw_othercenters", Sys.Date(), sep = "/")
system(command = paste("mkdir -p", base_dest_dir))

for (i in 1:nrow(sample_info)) {
  sample_number <- sample_info$sample_number[i]
  viollier_plate_name <- sample_info$viollier_plate_name[i]
  sample_name <- sample_info$sample_name[i]
  source_dir <- paste(PANGODIR, "backup/sampleset", sample_name, sep = "/")
  dest_dir <- paste(base_dest_dir, sample_number, sep = "/")

  if (is.na(sample_name) & !is.na(viollier_plate_name)) {
    sample_info[[i, "sample_status"]] <- "awaiting sequence"
    print(paste0(sample_number, ": Sequencing not finished yet."))
    next
  } else if (dir.exists(dest_dir)) {
    sample_info[[i, "sample_status"]] <- "available"
    print(paste0(sample_number, ": Already in D-BSSE outbox."))
  } else if (!is.na(sample_name) & !dir.exists(source_dir)) {
    sample_info[[i, "sample_status"]] <- "V-pipe sample directory not found"
    print(paste0(sample_number, ": V-pipe sample directory not found."))
    next
  } else if (!is.na(sample_name)) {
    system(command = paste("cp -r", source_dir, dest_dir))
    sample_info[[i, "sample_status"]] <- "available"
    print(paste0(sample_number, ": Copied to D-BSSE outbox."))
  }
}

filename <- paste0(Sys.Date(), "_seq_info.csv")
write.csv(sample_info, file = paste0("data/viollier_seq_requests/", filename))
write.csv(sample_info, file = paste(base_dest_dir, filename, sep = "/"))

if (continue == 'y') {
  print(paste("Running command:", upload_viollier_sync_command))
  system(command = sync_command)
} else {
  print("Not initiating synchronization.")
}



