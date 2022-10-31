FROM registry.ethz.ch/sars_cov_2/s3c/harvester-database-and-automation:base_image_r_and_python

# Install nextclade
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash -
RUN npm install --global @neherlab/nextclade

WORKDIR /app

ENTRYPOINT ["Rscript", "./R/automation/auto_import_nextclade_mutations.R"]
