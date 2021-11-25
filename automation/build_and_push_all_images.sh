#!/bin/bash
set -euo pipefail

# Constants
IMAGE_NAME=ghcr.io/cevo-public/harvester

# Clean up
rm -rf bag_meldeformular_importer/database
rm -rf bag_meldeformular_dashboard_importer/database
rm -rf consensus_sequence_importer/database
rm -rf gisaid_importer/database
rm -rf nextclade_importer/database
rm -rf owid_global_cases_importer/database
rm -rf pangolin_lineage_importer/database
rm -rf spsp_transferer/database
rm -rf gisaid_api_importer/database
rm -rf java/database

cp -r ../database bag_meldeformular_importer/database
cp -r ../database bag_meldeformular_dashboard_importer/database
cp -r ../database consensus_sequence_importer/database
cp -r ../database gisaid_importer/database
cp -r ../database nextclade_importer/database
cp -r ../database owid_global_cases_importer/database
cp -r ../database pangolin_lineage_importer/database
cp -r ../database spsp_transferer/database
cp -r ../database gisaid_api_importer/database
cp -r ../database java/database

# Fetch repository of pangolin lineages
cd pangolin_lineage_importer
rm -rf pangolin
git clone https://github.com/cov-lineages/pangolin.git
cd ..

# Fetch repository of SPSP transfer tool
cd spsp_transferer
rm -rf transfer-tool
git clone https://gitlab.sib.swiss/SPSP/transfer-tool.git
cd ..

# Build images
echo "--- Building bag_meldeformular_importer ---"
docker build -t $IMAGE_NAME:bag_meldeformular_importer bag_meldeformular_importer/
echo "--- Building bag_meldeformular_dashboard_importer ---"
docker build -t $IMAGE_NAME:bag_meldeformular_dashboard_importer bag_meldeformular_dashboard_importer/
echo "--- Building consensus_sequence_importer ---"
docker build -t $IMAGE_NAME:consensus_sequence_importer consensus_sequence_importer/
echo "--- Building gisaid_importer ---"
docker build -t $IMAGE_NAME:gisaid_importer gisaid_importer/
echo "--- Building nextclade_importer ---"
docker build -t $IMAGE_NAME:nextclade_importer nextclade_importer/
echo "--- Building pangolin_lineage_importer ---"
docker build -t $IMAGE_NAME:pangolin_lineage_importer pangolin_lineage_importer/
echo "--- Building polybox_updater ---"
docker build -t $IMAGE_NAME:polybox_updater polybox_updater/
echo "--- Building owid_global_cases_importer ---"
docker build -t $IMAGE_NAME:owid_global_cases_importer owid_global_cases_importer/
echo "--- Building spsp_transferer ---"
docker build -t $IMAGE_NAME:spsp_transferer spsp_transferer/
echo "--- Building gisaid_api_importer ---"
docker build -t $IMAGE_NAME:gisaid_api_importer gisaid_api_importer/
echo "--- Building java ---"
docker build -t $IMAGE_NAME:java java/
echo "--- Building viollier_metadata_receiver ---"
docker build -t $IMAGE_NAME:viollier_metadata_receiver viollier_metadata_receiver/

docker push $IMAGE_NAME:bag_meldeformular_importer
docker push $IMAGE_NAME:bag_meldeformular_dashboard_importer
docker push $IMAGE_NAME:consensus_sequence_importer
docker push $IMAGE_NAME:gisaid_importer
docker push $IMAGE_NAME:nextclade_importer
docker push $IMAGE_NAME:pangolin_lineage_importer
docker push $IMAGE_NAME:polybox_updater
docker push $IMAGE_NAME:owid_global_cases_importer
docker push $IMAGE_NAME:spsp_transferer
docker push $IMAGE_NAME:gisaid_api_importer
docker push $IMAGE_NAME:java
docker push $IMAGE_NAME:viollier_metadata_receiver
