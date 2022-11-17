FROM registry.ethz.ch/sars_cov_2/s3c/harvester-database-and-automation:base_image_r_and_python

WORKDIR /app
ENTRYPOINT ["/bin/bash", "-c", "Rscript ./R/automation/auto_import_bag_meldeformular.R && Rscript ./R/automation/auto_export_bag_sequence_report.R"]
