# This script is to import canton code to canton name mapping
# It's based on the Swiss government standard given on p. 81 of:
# https://www.bk.admin.ch/dam/bk/en/dokumente/sprachdienste/English%20Style%20Guide.pdf.download.pdf/english_style_guide.pdf

source("R/utility.R")
DB_INSTANCE <- "server"  # one of the database connection options in config.yml
TBL_NAME <- "swiss_canton"

names <- c(
  "Aargau", "Argovie", "Argovia", "Aargau",
  "Appenzell Ausserrhoden",
  "Appenzell RhodesExtérieures",
  "Appenzello Esterno", "Appenzell Ausserrhoden",
  "Appenzell Innerrhoden",
  "Appenzell RhodesIntérieures",
  "Appenzello Interno", "Appenzell Innerrhoden",
  "Basel-Landschaft", "Bâle-Campagne", "Basilea Campagna", "Basel-Landschaft",
  "Basel-Stadt", "Bâle-Ville", "Basilea Città", "Basel-Stadt",
  "Bern", "Berne", "Berna", "Bern",
  "Freiburg", "Fribourg", "Friburgo", "Fribourg",
  "Genf", "Genève", "Ginevra", "Geneva",
  "Glarus", "Glaris", "Glarona", "Glarus",
  "Graubünden", "Grisons", "Grigioni", "Graubünden",
  "Jura", "Jura", "Giura", "Jura",
  "Luzern", "Lucerne", "Lucerna", "Lucerne",
  "Neuenburg", "Neuchâtel", "Neuchâtel", "Neuchâtel",
  "Nidwalden", "Nidwald", "Nidvaldo", "Nidwalden",
  "Obwalden", "Obwald", "Obvaldo", "Obwalden",
  "Schaffhausen", "Schaffhouse", "Sciaffusa", "Schaffhausen",
  "Schwyz", "Schwyz", "Svitto", "Schwyz",
  "Solothurn", "Soleure", "Soletta", "Solothurn",
  "St. Gallen", "Saint-Gall", "San Gallo", "St Gallen",
  "Thurgau", "Thurgovie", "Turgovia", "Thurgau",
  "Tessin", "Tessin", "Ticino", "Ticino",
  "Uri", "Uri", "Uri", "Uri",
  "Wallis", "Valais", "Vallese", "Valais",
  "Waadt", "Vaud", "Vaud", "Vaud",
  "Zug", "Zoug", "Zugo", "Zug",
  "Zürich", "Zurich", "Zurigo", "Zurich")

names_df <- data.frame(
  canton_code = c("AG", "AR", "AI", "BL", "BS", "BE", "FR", "GE", "GL", "GR", "JU", 
           "LU", "NE", "NW", "OW", "SH", "SZ", "SO", "SG", "TG", "TI", "UR", 
           "VS", "VD", "ZG", "ZH"),
  german = names[seq(from = 1, to = length(names), by = 4)],
  french = names[seq(from = 2, to = length(names), by = 4)],
  italian = names[seq(from = 3, to = length(names), by = 4)],
  english = names[seq(from = 4, to = length(names), by = 4)])

db_connection <- open_database_connection(db_instance = DB_INSTANCE)
DBI::dbBegin(db_connection)
DBI::dbAppendTable(db_connection, name = TBL_NAME, names_df)
DBI::dbCommit(db_connection)
DBI::dbDisconnect(db_connection)



