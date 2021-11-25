# This script is to transform & load mapping between ethid and gisaid_epi_isl
# We rely on downloading the GISAID nextmeta
# resource and searching for our ethid's in this document. 

DATA <- "data/gisaid_metadata/metadata.tsv"
TYPE <- "gisaid"  # 'gisaid' means gisaid 'download packages metadata download, 'nextmeta' means gisaid "genomic epidemiology" metadata download, 'raw_download' means not metadata but Download > sequence metadata; assumption is these are all ETH-produced samples
TBL_NAME <- "sequence_identifier"

require(dplyr)
source("R/utility.R")

# Load data
data <- read.delim(file = DATA, sep = "\t", stringsAsFactors = F)

# Format data
if (TYPE == "raw_download") {
  our_sequence_data <- data %>% 
    rename(strain = Virus.name, gisaid_epi_isl = Accession.ID)
} else if (TYPE == "nextmeta") {
  our_sequence_data <- data %>%
    filter(submitting_lab == "Department of Biosystems Science and Engineering, ETH Zürich")
} else if (TYPE == "gisaid") {
  our_sequence_data <- data %>%
    filter(grepl(x = Virus.name, pattern = "ETHZ")) %>%
    rename(strain = Virus.name, gisaid_epi_isl = Accession.ID)
}
our_sequence_data$ethid <- unlist(lapply(
  X = our_sequence_data$strain,
  FUN = get_ethid_from_gisaid_strain))
our_sequence_data <- our_sequence_data %>%
  filter(!is.na(ethid)) %>%
  rename(gisaid_id = gisaid_epi_isl) %>%
  select(gisaid_id, ethid)

# Check for duplicate ethid's
duplicated_ethid_data <- our_sequence_data %>%
  group_by(ethid) %>%
  filter(n() > 1) %>%
  arrange(ethid)
if (nrow(duplicated_ethid_data) > 0) {
  stop("There are duplicate ethid's in metadata file", DATA, ":\n",
       paste0(duplicated_ethid_data$strain, collapse = "\n"))
}

# Connect to database
db_connection <- open_database_connection(db_instance = "server")

# Enforce table specifications,
# if fails returns NA table which can't be appended to database table
gisaid_ids <- tryCatch(
  {
    enforce_sql_spec(
      table = our_sequence_data,
      table_name = TBL_NAME,
      db_connection = db_connection)
  },
  error = function(cond) {
    message(cond)
    return(NA)
  }
)

# Coalesce join rather than overwriting sample_names
prev_table <- dplyr::tbl(db_connection, TBL_NAME) %>% collect()
joined_table <- coalesce_join(
  x = prev_table,
  y = gisaid_ids,
  by = "ethid")

table_spec <- parse_table_specification(
  table_name = TBL_NAME, db_connection = db_connection)

update_table(
  table_name = TBL_NAME, 
  new_table = joined_table, 
  con = db_connection, 
  append_new_rows = T,
  cols_to_update = "gisaid_id", 
  key_col = "ethid", 
  table_spec = table_spec)
