#' Import COVID-19 incidence data from OWID.
#' @param db_connection
import_global_incidence <- function(
  db_connection, table_name = "ext_owid_global_cases"
) {
  SOURCE_LINK <- "https://covid.ourworldindata.org/data/owid-covid-data.csv"
  cases <- read.csv(url(SOURCE_LINK)) %>%
    select(location, date, new_cases_per_million,
           new_deaths_per_million, new_cases, new_deaths) %>%
    mutate(iso_country = country_name_to_iso_code(location)) %>%
    filter(!is.na(iso_country)) %>%
    mutate(iso_country = get_standardized_iso_country(
      iso_country = iso_country, db_connection = db_connection)) %>%
    group_by(iso_country, date) %>%
    summarize(
      country = paste0(unique(location), collapse = ", "),
      new_cases_per_million = mean(new_cases_per_million, na.rm = T),
      new_deaths_per_million = mean(new_deaths_per_million, na.rm = T),
      new_cases = sum(new_cases, na.rm = T),
      new_deaths = sum(new_deaths, na.rm = T))

  key_col <- c("iso_country", "date")
  cols_to_update <- colnames(cases)[!(colnames(cases) %in% key_col)]
  table_spec <- parse_table_specification(table_name = table_name,
                                          db_connection = db_connection)
  update_table(table_name = table_name,
               new_table = cases,
               con = db_connection,
               append_new_rows = T,
               cols_to_update = cols_to_update,
               key_col = key_col,
               table_spec = table_spec)
}


#' Import COVID-19 incidence data from OWID for CoV-Spectrum.
#' @param db_connection
import_global_incidence_cov_spectrum <- function(
  db_connection, table_name = "spectrum_owid_global_cases_raw"
) {
  SOURCE_LINK <- "https://covid.ourworldindata.org/data/owid-covid-data.csv"
  cases <- read.csv(url(SOURCE_LINK)) %>%
    select(iso_code, continent, location, date, new_cases_per_million,
           new_deaths_per_million, new_cases, new_deaths) %>%
    rename(
      iso_country = iso_code,
      region = continent,
      country = location
    ) %>%
    filter(!is.na(region) & region != "") %>%
    filter(!is.na(country) & region != "")

  key_col <- c("country", "date")
  cols_to_update <- colnames(cases)[!(colnames(cases) %in% key_col)]
  table_spec <- parse_table_specification(table_name = table_name,
                                          db_connection = db_connection)
  update_table(table_name = table_name,
               new_table = cases,
               con = db_connection,
               append_new_rows = T,
               cols_to_update = cols_to_update,
               key_col = key_col,
               table_spec = table_spec)
}
