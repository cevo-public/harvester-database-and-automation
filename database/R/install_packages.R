# Try installing libraries to default library location
# If that fails (no write permission, e.g. on Euler) try to find the environment varible R_LIBS_USER giving a write-able library location

packages.default <- c(
    "tidyverse",
    "glue",
    "DBI",
    "RPostgres",
    "askpass",
    "rnaturalearth",
    "config",
    "emayili",
    "countrycode",
    "argparse",
    "cellranger",
    "BiocManager"
)

packages.biocmanager <- c("Biostrings" )

packages.all <- c(packages.default, packages.biocmanager)


install_on_error_or_warning <- function(err) {
  print(err)
  lib_loc <- Sys.getenv("R_LIBS_USER")
  if (lib_loc != "") {
    print(paste("Installing packages into:", lib_loc))
    for (p in packages.default) {
        install.packages(p, lib = lib_loc)
    }
    for (p in packages.biocmanager) {
        BiocManager::install(p, lib = lib_loc)
    }
  } else {
    print("Couldn't find a different library specified in environment variable R_LIBS_USER!")
  }
}

args <- commandArgs(trailingOnly=T)
print(args)

if (length(args) == 0 | args[1] == "install") {

        out <- tryCatch(
        {
          message("Trying to install R packages in default library")
          for (p in packages.default) {
                install.packages(p)
          }
          for (p in packages.biocmanager) {
                BiocManager::install(p)
          }

        },
          error=install_on_error_or_warning,
          warning=install_on_error_or_warning
        )
} else if (args[1] == "check") {
        result <- T
        out <- tryCatch(
        {
          for (p in packages.all) {
                library(p, character.only=TRUE)
          }
        },
          error=function(err){print(err); quit(status=1)},
          warning=function(err){print(err); quit(status=1)}
        )
    quit(status=0);
}
