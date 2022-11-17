FROM registry.ethz.ch/sars_cov_2/s3c/harvester-database-and-automation:base_image_r_and_python

WORKDIR /app
ENTRYPOINT ["Rscript", "./R/automation/auto_owid_global_cases_importer.R"]
