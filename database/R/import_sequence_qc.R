# This script is to update table "consensus_sequence" with sequencing qc column 
# 'fail_reason'. This script DOES NOT mark duplicate sequences or distinguish 
# between clinical samples and controls, etc.

# QC consensus_sequence table entries that are not yet QC'd
import_sequence_qc <- function(
  db_connection, min_completion = 20000, max_excess_divergence = 15, 
  fail_frameshifts = F, overwrite_prev_qc = T) {
  query <- dplyr::tbl(db_connection, "consensus_sequence") %>%
    mutate(
      fail_reason = case_when(
        number_n >= 29903 - min_completion ~ paste("<", min_completion, "non-N bases"),
        !is.null(clusters) ~ flagging_reason,
        excess_divergence > max_excess_divergence ~ paste(">", max_excess_divergence, "excess mutations"),
        is.na(number_n) ~ "missing number_n from diagnostic.py!",
        T ~ "no fail reason")) %>%
    select(sample_name, ethid, fail_reason, number_n, sequencing_center, gaps)
  qcd_tbl <- query %>% collect()
  
  if (fail_frameshifts) {
    qcd_tbl$has_frameshift <- unlist(lapply(
      X = qcd_tbl$gaps,
      FUN = get_has_frameshift_mutation))
    qcd_tbl <- qcd_tbl %>% mutate(
      fail_reason = case_when(
        fail_reason == "no fail reason" & has_frameshift ~ "has frameshift deletion",
        T ~ fail_reason))
  }
  
  # Update table with QC result
  if (overwrite_prev_qc) {
    joined_fail_reasons <- qcd_tbl
  } else {
    prev_fail_reasons <-  query <- dplyr::tbl(db_connection, "consensus_sequence") %>%
      select(sample_name, fail_reason) %>%
      collect()
    joined_fail_reasons <- coalesce_join(
      x = prev_fail_reasons,
      y = qcd_tbl,
      by = "sample_name")
  }
  
  table_spec <- parse_table_specification(
    table_name = "consensus_sequence", db_connection = db_connection)
  
  update_table(
    table_name = "consensus_sequence", 
    new_table = joined_fail_reasons, 
    con = db_connection,
    append_new_rows = F, 
    cols_to_update = c("fail_reason"),
    key_col = "sample_name",
    table_spec = table_spec)
}
