# Try installing libraries to default library location
# If that fails (no write permission, e.g. on Euler) try to find the environment varible R_LIBS_USER giving a write-able library location

install_on_error_or_warning <- function() {
  lib_loc <- Sys.getenv("R_LIBS_USER")
  if (lib_loc != "") {
    print(paste("Installing packages into:", lib_loc))

    install.packages("tidyverse", lib = lib_loc)
    install.packages("glue", lib = lib_loc)
    install.packages("DBI", lib = lib_loc)
    install.packages("RPostgres", lib = lib_loc)
    install.packages("askpass", lib = lib_loc)
    install.packages("rnaturalearth", lib = lib_loc)
    install.packages("rgeos", lib = lib_loc)
    install.packages("config", lib = lib_loc)
    install.packages("emayili", lib = lib_loc)
    install.packages("countrycode", lib = lib_loc)
    install.packages("argparse", lib = lib_loc)
    install.packages("cellranger", lib = lib_loc)
    install.packages("BiocManager", lib = lib_loc)
    BiocManager::install("Biostrings", lib = lib_loc)

  } else {
    print("Couldn't find a different library specified in environment variable R_LIBS_USER!")
  }
}

out <- tryCatch(
{
  message("Trying to install R packages in default library")
  install.packages("tidyverse")
  install.packages("glue")
  install.packages("DBI")
  install.packages("RPostgres")
  install.packages("askpass")
  install.packages("rnaturalearth")
  install.packages("rgeos")
  install.packages("config")
  install.packages("emayili")
  install.packages("countrycode")
  install.packages("argparse")
  install.packages("cellranger")
  install.packages("BiocManager")
  BiocManager::install("Biostrings")
},
  error=install_on_error_or_warning,
  warning=install_on_error_or_warning
)




