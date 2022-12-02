#!/bin/bash
set -euo pipefail

for component in \
	bag_meldeformular_importer \
	bag_meldeformular_dashboard_importer \
	sequence_diagnostic_importer \
	gisaid_importer nextclade_importer \
	owid_global_cases_importer \
	pangolin_lineage_importer \
	spsp_transferer \
	gisaid_api_importer \
	java \
	viollier_metadata_receiver
do
	./build_and_push_image.sh $component
done
