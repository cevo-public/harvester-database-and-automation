FROM rocker/tidyverse:4.0.3
WORKDIR /app/

# Update packages
RUN apt-get update && apt-get -y upgrade

# Install R packages
COPY database/R/install_packages.R R/
RUN Rscript R/install_packages.R

# Install nextclade
RUN apt-get update && apt-get install -y curl
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash -
RUN apt-get install -y nodejs
RUN npm install --global @neherlab/nextclade

COPY database/ .

ENTRYPOINT ["Rscript", "R/automation/auto_import_nextclade_mutations.R"]
