FROM rocker/tidyverse:4.0.3
WORKDIR /app/

# Update packages
RUN apt-get update && apt-get -y upgrade

# Install R packages
COPY database/R/install_packages.R R/
RUN Rscript R/install_packages.R

COPY database/ .

ENTRYPOINT Rscript R/automation/auto_owid_global_cases_importer.R
