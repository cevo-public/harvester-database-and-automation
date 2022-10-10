FROM registry.ethz.ch/sars_cov_2/s3c/harvester-database-and-automation:base_image_r_and_python

# Install nextclade
RUN apt-get update && apt-get install -y curl nodejs
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash -
RUN npm install --global @nextstrain/nextclade@0.14.4

ENTRYPOINT ["Rscript", "/app/R/automation/auto_import_nextclade_mutations.R"]
