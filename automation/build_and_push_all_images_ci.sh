#!/bin/sh
set -eu

for component in bag_meldeformular_importer \
    bag_meldeformular_dashboard_importer \
    consensus_sequence_importer \
    gisaid_importer \
    nextclade_importer \
    owid_global_cases_importer \
    pangolin_lineage_importer \
    gisaid_api_importer \
    viollier_metadata_receiver \
    java; do
    echo .
    echo .
    echo .
    echo "---------------------------------------------------------------------------------"
    echo .
    ./build_and_push_kaniko.sh $component;
done
