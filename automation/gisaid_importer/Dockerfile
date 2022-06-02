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

# Install mafft
RUN wget https://mafft.cbrc.jp/alignment/software/mafft_7.475-1_amd64.deb
RUN dpkg -i mafft_7.475-1_amd64.deb
RUN rm mafft_7.475-1_amd64.deb
RUN which mafft

COPY database/ .
COPY reference.fasta .
COPY align_splits.sh .
COPY insert_data.sh .
COPY nextclade_splits.sh .
RUN chmod +x align_splits.sh nextclade_splits.sh

ENTRYPOINT cd /app && Rscript R/automation/auto_import_gisaid.R --main
