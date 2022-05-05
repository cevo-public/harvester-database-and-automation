#!/bin/bash
set -euo pipefail

for component in bag_meldeformular_importer \
	bag_meldeformular_dashboard_importer consensus_sequence_importer \
	gisaid_importer nextclade_importer owid_global_cases_importer \
	pangolin_lineage_importer spsp_transferer gisaid_api_importer java; do
	./build_and_push_all_images.sh $component;
done
