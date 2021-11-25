options(warn = 2)

library(glue)
library(tidyverse)
library(lubridate)

source('R/utility.R')

# Makes sure that a config file exist
tryCatch({
  config::get()
}, error = function (e) {
  stop("No config file could be found. Please create a config.yml. An example config is provided at config.example.yml")
})
