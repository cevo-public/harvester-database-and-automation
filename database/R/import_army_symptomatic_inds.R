source("R/utility.R")
library(xlsx)
db_connection <- open_database_connection("server")

symptomatic_sample_numbers <- xlsx::read.xlsx(
  file = "data/army_metadata/Viollier-Auftragsnummern der positiven Proben symptomatischer AdA KW 2_3und6.xlsx",
  sheetIndex = 1,
  header = F
)

symptomatic_sample_numbers_clean <- gsub(
  x = symptomatic_sample_numbers$X2,
  pattern = "\\.",
  replacement = ""
)

symptomatic_sample_numbers_clean <- as.numeric(symptomatic_sample_numbers_clean)
symptomatic_sample_numbers_clean <- symptomatic_sample_numbers_clean[!is.na(symptomatic_sample_numbers_clean)]
print(paste(length(symptomatic_sample_numbers_clean), "sample numbers found in data source."))

symptomatic_sample_data <- dplyr::left_join(
  x = dplyr::tbl(db_connection, "viollier_test") %>%
    filter(sample_number %in% !! symptomatic_sample_numbers_clean) %>%
    select(sample_number, ethid),
  y = dplyr::tbl(db_connection, "consensus_sequence") %>%
    select(sample_name, ethid),
  by = "ethid") %>% 
  collect() %>%
  mutate(comment = "Symptomatic Army")

sample_name_na <- symptomatic_sample_data %>% 
  filter(is.na(sample_name))
if (nrow(sample_name_na) > 0) {
  warning("Not adding ", nrow(sample_name_na), " out of ",
          nrow(symptomatic_sample_data), " samples to table because no seq.")
  symptomatic_sample_data <- symptomatic_sample_data %>% 
    filter(!is.na(sample_name))
}

table_spec <- parse_table_specification(
  table_name = "x_consensus_sequence_notes",
  db_connection = db_connection
)
update_table(
  table_name = "x_consensus_sequence_notes",
  new_table = symptomatic_sample_data,
  con = db_connection,
  cols_to_update = c("ethid", "comment"),
  key_col = "sample_name",
  table_spec = table_spec
)
  

