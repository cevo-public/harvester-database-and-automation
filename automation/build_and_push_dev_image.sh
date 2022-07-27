#!/bin/bash
set -euo pipefail

# Constants
IMAGE_NAME=registry.ethz.ch/sars_cov_2/harvester-database-and-automation

component=${1:-}
test -z $component && { echo "you must specify folder as first argument"; exit 1; }
test -d $component || { echo "no folder $component"; exit 1; }

# Clean up
rm -r $component/database
cp -r ../database $component/database

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
echo "--- Building $component ---"
docker build -t $IMAGE_NAME:${component}_dev $component/

echo "--- push $component ---"
docker push $IMAGE_NAME:${component}_dev
