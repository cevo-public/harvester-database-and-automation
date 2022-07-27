#!/bin/bash
set -euo pipefail

# Constants
IMAGE_NAME=registry.ethz.ch/sars_cov_2/harvester-database-and-automation

component=$1

# Clean up
rm -rf $component/database

cp -r ../database $component/database

# Fetch repository of pangolin lineages
if [[ $component = pangolin_lineage_importer ]]; then
    pushd pangolin_lineage_importer
    rm -rf pangolin
    git clone https://github.com/cov-lineages/pangolin.git
    popd
fi

if [[ $component = spsp_transferer ]]; then
    # Fetch repository of SPSP transfer tool
    pushd spsp_transferer
    rm -rf transfer-tool
    git clone https://gitlab.sib.swiss/SPSP/transfer-tool.git
    popd
fi

# Build images
echo "--- Building $component ---"
docker build -t $IMAGE_NAME:$component $component/

echo "--- push $component ---"
docker push $IMAGE_NAME:$component
