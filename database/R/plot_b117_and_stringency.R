uk <- function(db_connection, start_date, end_date) {
  country <- "United Kingdom"

  data_oxford <- dplyr::tbl(db_connection, "oxford_policy_tracker") %>%
    filter(
      country_name == country
        & jurisdiction == "NAT_TOTAL"
        & date >= start_date
        & date <= end_date
    ) %>%
    arrange(date) %>%
    select(date, stringency_index) %>%
    collect()

  data_b117 <- read_csv(
    "https://raw.githubusercontent.com/covid-19-Re/variantPlot/master/data/data_comparison.csv",
    col_types = cols(
      country = col_character(),
      year = col_integer(),
      week = col_integer(),
      n = col_integer(),
      b117 = col_integer()
    )
  ) %>%
    filter(country == "UK S-dropout") %>%
    mutate(proportion = b117 / n) %>%
    select(year, week, proportion)

  data_cases <- dplyr::tbl(db_connection, "owid_global_cases") %>%
    filter(
      location == country
        & date >= start_date
        & date <= end_date
    ) %>%
    mutate(
      year = as.integer(date_part("isoyear", date)),
      week = as.integer(date_part("week", date))
    ) %>%
    group_by(year, week) %>%
    summarize(
      date = min(date, na.rm = TRUE),
      new_cases = as.integer(sum(new_cases, na.rm = TRUE))
    ) %>%
    arrange(year, week) %>%
    collect()

  data <- data_b117 %>%
    inner_join(data_cases, by = c("year", "week"))

  # To use the same time frame
  data_oxford <- data_oxford %>%
    inner_join(data, by = "date") %>%
    select(date, stringency_index)

  axis_scale_coeff <- max(data$new_cases)

  p1 <- ggplot(data, aes(x = date)) +
    geom_line(aes(y = new_cases), color = "#67916E", size = 2) +
    geom_point(aes(y = new_cases), color = "#67916E", size = 4) +
    geom_line(aes(y = proportion * axis_scale_coeff), color = "#0D4A70", size = 2) +
    geom_point(aes(y = proportion * axis_scale_coeff), color = "#0D4A70", size = 4) +
    scale_y_continuous(
      name = "Number of cases per week",
      sec.axis = sec_axis(~. / axis_scale_coeff, name = "Proportion of B.1.1.7"),
      labels = scales::comma
    ) +
    theme_light() +
    theme(
      text = element_text(size = 20),
      axis.title.y = element_text(colour = "#67916E"),
      axis.title.y.right = element_text(colour = "#0D4A70")
    ) +
    ggtitle("United Kingdom, S-dropouts as proxy for B.1.1.7")

  p2 <- ggplot(data_oxford, aes(x = date, y = stringency_index)) +
    geom_step(size = 1) +
    scale_y_continuous(
      name = "Stringency index"
    ) +
    theme_light() +
    theme(
      text = element_text(size = 20)
    )

  gA <- ggplotGrob(p1)
  gB <- ggplotGrob(p2)
  maxWidth <- grid::unit.pmax(gA$widths[2:5], gB$widths[2:5])
  gA$widths[2:5] <- as.list(maxWidth)
  gB$widths[2:5] <- as.list(maxWidth)
  pgrid <- gridExtra::grid.arrange(
    gA, gB,
    nrow = 2,
    heights = c(2, 1)
  )

  ggsave("b117_and_stringency_in_uk.pdf",
         pgrid, device = "pdf", width = 10, height = 7)
}


ireland <- function (db_connection, start_date, end_date) {
  country <- "Ireland"

  data_oxford <- dplyr::tbl(db_connection, "oxford_policy_tracker") %>%
    filter(
      country_name == country
        & jurisdiction == "NAT_TOTAL"
        & date >= start_date
        & date <= end_date
    ) %>%
    arrange(date) %>%
    select(date, stringency_index) %>%
    collect()

  data_b117 <- read_csv(
    "data/ireland_sn501y_gisaid.csv",
    col_types = cols(
      year = col_integer(),
      week = col_integer(),
      n = col_integer(),
      b117 = col_integer()
    )
  ) %>%
    mutate(proportion = b117 / n) %>%
    select(year, week, proportion)

  data_cases <- dplyr::tbl(db_connection, "owid_global_cases") %>%
    filter(
      location == country
        & date >= start_date
        & date <= end_date
    ) %>%
    mutate(
      year = as.integer(date_part("isoyear", date)),
      week = as.integer(date_part("week", date))
    ) %>%
    group_by(year, week) %>%
    summarize(
      date = min(date, na.rm = TRUE),
      new_cases = as.integer(sum(new_cases, na.rm = TRUE))
    ) %>%
    arrange(year, week) %>%
    collect()

  data <- data_b117 %>%
    inner_join(data_cases, by = c("year", "week"))

  # To use the same time frame
  data_oxford <- data_oxford %>%
    inner_join(data, by = "date") %>%
    select(date, stringency_index)

  axis_scale_coeff <- max(data$new_cases)

  p1 <- ggplot(data, aes(x = date)) +
    geom_line(aes(y = new_cases), color = "#67916E", size = 2) +
    geom_point(aes(y = new_cases), color = "#67916E", size = 4) +
    geom_line(aes(y = proportion * axis_scale_coeff), color = "#0D4A70", size = 2) +
    geom_point(aes(y = proportion * axis_scale_coeff), color = "#0D4A70", size = 4) +
    scale_y_continuous(
      name = "Number of cases per week",
      sec.axis = sec_axis(~. / axis_scale_coeff, name = "Proportion of B.1.1.7"),
      labels = scales::comma
    ) +
    theme_light() +
    theme(
      text = element_text(size = 20),
      axis.title.y = element_text(colour = "#67916E"),
      axis.title.y.right = element_text(colour = "#0D4A70")
    ) +
    ggtitle("Ireland, S:N501Y from sequencing as proxy for B.1.1.7")

  p2 <- ggplot(data_oxford, aes(x = date, y = stringency_index)) +
    geom_step(size = 1) +
    scale_y_continuous(
      name = "Stringency index"
    ) +
    theme_light() +
    theme(
      text = element_text(size = 20)
    )

  gA <- ggplotGrob(p1)
  gB <- ggplotGrob(p2)
  maxWidth <- grid::unit.pmax(gA$widths[2:5], gB$widths[2:5])
  gA$widths[2:5] <- as.list(maxWidth)
  gB$widths[2:5] <- as.list(maxWidth)
  pgrid <- gridExtra::grid.arrange(
    gA, gB,
    nrow = 2,
    heights = c(2, 1)
  )

  ggsave("b117_and_stringency_in_ireland.pdf",
         pgrid, device = "pdf", width = 10, height = 7)
}


main <- function() {
  db_connection <- open_database_connection("server")
  start_date <- ymd(20201005)
  end_date <- ymd(20210131)

  uk(db_connection, start_date, end_date)
  ireland(db_connection, start_date, end_date)
  DBI::dbDisconnect(db_connection)
}
