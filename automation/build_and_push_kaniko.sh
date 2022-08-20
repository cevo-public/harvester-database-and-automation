#!/bin/sh
set -eu

# Constants
IMAGE_NAME=$CI_REGISTRY_IMAGE

component=$1
suffix=-${CI_COMMIT_BRANCH}

echo build $component with suffix $suffix
echo .

cp -r ../database $component/database

# Fetch repository of pangolin lineages
if [[ $component = pangolin_lineage_importer ]]; then
    cp -R ../pangolin pangolin_lineage_importer
fi

if [[ $component = spsp_transferer ]]; then
    # Fetch repository of SPSP transfer tool
    cp -R ../transfer-tool spsp_transferer
fi


/kaniko/executor --context ${component} --dockerfile ${component}/Dockerfile --destination $IMAGE_NAME:${component}${suffix} --cache=false
