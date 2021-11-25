
#' Used to import Demographic balance by age and canton (px-x-0102020000_104)
#' Provided by the Swiss Federal Statistical Office
#' https://www.pxweb.bfs.admin.ch/pxweb/en/px-x-0102020000_104/px-x-0102020000_104/px-x-0102020000_104.px
#'
#' The input file must be encoded as UTF-8.
#' Attention: 99 years or older are transformed to 99 years.
import_switzerland_demographic_data <- function (path_to_data, db_connection) {
  data <- read_tsv(
    path_to_data,
    skip = 2
  ) %>%
    rename(
      demographic_component = "Demographic component",
      canton = "Canton",
      citizenship = "Citizenship (category)",
      sex = "Sex",
      age = "Age"
    ) %>%
    pivot_longer(-(demographic_component:age), names_to = "year", values_to = "count") %>%
    mutate(
      count = as.integer(count),
      age = as.integer(str_replace(age, " year.*", ""))
    ) %>%
    select(!citizenship)

  canton_map <- tibble(
    name = c("Aargau", "Appenzell Ausserrhoden", "Appenzell Innerrhoden", "Basel-Landschaft", "Basel-Stadt",
             "Bern / Berne", "Fribourg / Freiburg", "Genève", "Glarus", "Graubünden / Grigioni / Grischun", "Jura",
             "Luzern", "Neuchâtel", "Nidwalden", "Obwalden", "Schaffhausen", "Schwyz", "Solothurn", "St. Gallen",
             "Thurgau", "Ticino", "Uri", "Valais / Wallis", "Vaud", "Zug", "Zürich"
    ),
    abbreviation = c("AG", "AR", "AI", "BL", "BS", "BE", "FR", "GE", "GL", "GR", "JU", "LU", "NE", "NW", "OW", "SH",
                     "SZ", "SO", "SG", "TG", "TI", "UR", "VS", "VD", "ZG", "ZH"
    )
  )

  sex_map <- tibble(
    old_name = c("Man", "Woman"),
    new_name = c("Männlich", "Weiblich")
  )

  data <- data %>%
    left_join(canton_map, by = c("canton" = "name")) %>%
    left_join(sex_map, by = c("sex" = "old_name")) %>%
    mutate(
      canton = abbreviation,
      sex = new_name
    ) %>%
    select(demographic_component, canton, sex, age, year, count)

  DBI::dbBegin(db_connection)
  DBI::dbAppendTable(db_connection, "switzerland_demographic", data)
  DBI::dbCommit(db_connection)
}


#' Imports a list of countries from a prepared country names list and from rnaturalearth's dataset.
import_country_data <- function (db_connection) {
  data <- read_csv("data/country-names-german.csv") %>%
    full_join(rnaturalearth::ne_countries(returnclass = "sf"), by = c("iso3166_alpha3_code" = "iso_a3")) %>%
    rename(english_name = admin) %>%
    drop_na(iso3166_alpha3_code) %>%
    select(iso3166_alpha3_code, german_name, english_name)
  DBI::dbBegin(db_connection)
  DBI::dbAppendTable(db_connection, "country", data)
  DBI::dbCommit(db_connection)
}


# db_connection <- open_database_connection()
# import_switzerland_demographic_data("data/px-x-0102020000_104.csv", db_connection)
# import_country_data(db_connection)
# DBI::dbDisconnect(db_connection)
