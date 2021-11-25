source("R/utility.R")
db_connection <- open_database_connection("server")

ct_threshold_data_sql <- "select vt.ethid, fail_reason, consensus_n, e_gene_ct, rdrp_gene_ct, sequencing_center
from consensus_sequence cs
left join viollier_test vt on cs.ethid = vt.ethid
left join viollier_test__viollier_plate vtvp on vt.sample_number = vtvp.sample_number
where vt.ethid is not null;"

res <- DBI::dbSendQuery(conn = db_connection, statement = ct_threshold_data_sql)
ct_threshold_data <- DBI::dbFetch(res = res)
DBI::dbClearResult(res = res)

ct_threshold_data_1 <- ct_threshold_data %>%
  group_by(ethid) %>%
  arrange(consensus_n) %>%
  mutate(completeness_idx = 1:n()) %>%
  summarize(
    e_gene_ct = min(e_gene_ct, na.rm = T),
    rdrp_gene_ct = min(rdrp_gene_ct, na.rm = T),
    fail_reason = case_when(
      any(fail_reason == "no fail reason") ~ "no fail reason",
      T ~ "failed"),
    sequencing_center = sequencing_center[completeness_idx == 1])

ct_threshold_data_2 <- ct_threshold_data_1 %>%
  mutate(
    min_ct_value = pmin(e_gene_ct, rdrp_gene_ct, na.rm = T),
    min_ct_category = case_when(
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) == 0 ~ "= 0",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) < 5 ~ "< 5",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) < 10 ~ "5 - 10",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) < 15 ~ "10 - 15",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) < 20 ~ "15 - 20",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) < 25 ~ "20 - 25",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) < Inf ~ "25 - Inf",
      as.numeric(pmin(e_gene_ct, rdrp_gene_ct, na.rm = T)) == Inf ~ "unknown"
    ),
    min_ct_category = factor(
      x = min_ct_category, 
      levels = c("= 0", "< 5", "5 - 10", "10 - 15", "15 - 20", "20 - 25", "25 - Inf", "unknown")
    )
  )

# Plot success rates by sequencing center and CT values
ct_threshold_summary <- ct_threshold_data_2 %>% 
  group_by(min_ct_category, sequencing_center) %>%
  summarize(
    percent_pass = sum(fail_reason == "no fail reason") / n(),
    n_samples = n())
ggplot(
  data = ct_threshold_summary,
  aes(x = min_ct_category, y = percent_pass)) + 
  geom_point() + 
  geom_text(aes(label = paste0(round(percent_pass * 100, digits = 0), "%\n(n=", n_samples, ")")), vjust = 0.5) +
  lims(y = c(0, 1.1)) + 
  facet_grid(sequencing_center ~ .)

# What % of samples do we have CT values for?
sum(ct_threshold_data_2$min_ct_category == "unknown") / nrow(ct_threshold_data_2)

# Propose a filtering scheme, check overall resulting success rate
max_ct <- 20
min_ct <- 5
filtered_data <- ct_threshold_data_2 %>%
  filter(min_ct_value >= min_ct, min_ct_value < max_ct)
sum(filtered_data$fail_reason == "no fail reason") / nrow(filtered_data)

# Summarize overall success rates by sequencing center
ct_threshold_data_2 %>% 
  group_by(sequencing_center) %>%
  summarise(
    percent_pass = sum(fail_reason == "no fail reason") / n(),
    n_samples = n())
