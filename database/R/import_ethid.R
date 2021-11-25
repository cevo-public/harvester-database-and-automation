# This script is to import column ethid into table consensus_sequence based on
# column sample_name. It DOES NOT overwrite ETHIDs already in the database
# because these are sometimes manually changed (e.g. when GFB and Geneva accidentally got the same ETHIDs).

#' Import column ethid into table consensus_sequence based on column sample_name.
#' @param db_connection
import_ethid <- function(db_connection) {
  # Get sample names for which no ethid is assigned.
  sample_names_no_ethid <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    filter(is.na(ethid)) %>%
    select(sample_name) %>%
    collect()
  
  # Parse the ethid (if any) from the sample name
  sample_names_no_ethid$ethid <- unlist(lapply(
    X = sample_names_no_ethid$sample_name,
    FUN = get_ethid_from_sample_name,
    db_connection = db_connection))
  
  # Update the table accordingly
  table_spec <- parse_table_specification(
    table_name = "consensus_sequence", db_connection = db_connection)
  update_table(
    table_name = "consensus_sequence",
    new_table = sample_names_no_ethid,
    con = db_connection,
    append_new_rows = F,
    cols_to_update = "ethid",
    key_col = "sample_name",
    table_spec = table_spec,
    run_summarize_update = T)
} 
