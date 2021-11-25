#!/bin/bash
set -euo pipefail


NUMBER_WORKERS=32
i=1
for FILE in /app_wkdir/splits/*.fasta; do
    # This line ensures that at most NUMBER_WORKERS processes are run in parallel. The jobs are run in
    # NUMBER_WORKERS-sized batches, this means that not always NUMBER_WORKERS processes will be running.
    if ((i==0)) ; then
      wait
    fi
    i=$(((i+1)%NUMBER_WORKERS))

    FN=$(basename -- "$FILE")
    FN="${FN%.fasta}"
    Rscript R/automation/auto_import_gisaid.R --import-split $FN &
done
wait
