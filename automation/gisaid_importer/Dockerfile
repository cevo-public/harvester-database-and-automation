FROM registry.ethz.ch/sars_cov_2/s3c/harvester-database-and-automation:base_image_r_and_python

# Install nextclade
RUN curl -sL https://deb.nodesource.com/setup_lts.x | bash -
RUN npm install --global @neherlab/nextclade

# Install mafft
RUN wget https://mafft.cbrc.jp/alignment/software/mafft_7.475-1_amd64.deb \
    && dpkg -i mafft_7.475-1_amd64.deb \
    && rm mafft_7.475-1_amd64.deb \
    && which mafft

COPY reference.fasta align_splits.sh insert_data.sh nextclade_splits.sh /app
RUN chmod +x /app/align_splits.sh /app/nextclade_splits.sh

ENTRYPOINT ["/bin/bash", "-c", "cd /app && Rscript R/automation/auto_import_gisaid.R --main"]
