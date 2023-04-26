#!/bin/sh

# Fail on any error.
set -euo pipefail

# Build version-tagged Docker image for each component.
for component in \
    bag_meldeformular_importer \
    bag_meldeformular_dashboard_importer \
    gisaid_importer \
    gisaid_api_importer \
    nextclade_importer \
    owid_global_cases_importer \
    pangolin_lineage_importer \
    pangolin_lineage_exporter \
    polybox_updater \
    sequence_diagnostic_importer \
    spsp_exporter \
    viollier_metadata_receiver
do
    echo .
    echo .
    echo .
    echo "-------------------------------------------------------------------------------"
    echo Building component: $component
    echo .
    cp -r ${CI_PROJECT_DIR}/database ${CI_PROJECT_DIR}/automation/$component/database

    if [[ $component = pangolin_lineage_importer ]]; then
        # Fetch repository of pangolin lineages, provided as an artifact.
        cp -R ${CI_PROJECT_DIR}/pangolin ${CI_PROJECT_DIR}/automation/$component
    fi

    /kaniko/executor \
        --context ${CI_PROJECT_DIR}/automation/$component \
        --dockerfile ${CI_PROJECT_DIR}/automation/$component/Dockerfile \
        --destination $CI_REGISTRY_IMAGE:$component-$CI_COMMIT_TAG

done
