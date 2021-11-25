# This script is to import lat/long coordinates for countries around the globe
# taken from the R package CoordinateCleaner's data, which they say they get from 
# http://geo-locate.org. Since there are multiple entries per country, I take 
# the average coordinates.

table_name <- "ext_country_coordinates"
source("R/utility.R")
db_connection <- open_database_connection("server")

require(CoordinateCleaner)
data(countryref)
coordinates <- countryref

coordinates_transformed <- coordinates %>%
  filter(type == "country") %>%
  rename("longitude" = "centroid.lon", "latitude" = "centroid.lat",
         "iso_code" = "iso3") %>%
  group_by(iso_code) %>%
  summarize(latitude = mean(latitude),
            longitude = mean(longitude))

key_col = c("iso_code")
update_cols <- colnames(coordinates_transformed)[!(colnames(coordinates_transformed) %in% key_col)]
table_spec <- parse_table_specification(table_name = table_name,
                                        db_connection = db_connection)
update_table(table_name = table_name, 
             new_table = coordinates_transformed, 
             con = db_connection, 
             append_new_rows = T,
             cols_to_update = update_cols, 
             key_col = key_col, 
             table_spec = table_spec)


