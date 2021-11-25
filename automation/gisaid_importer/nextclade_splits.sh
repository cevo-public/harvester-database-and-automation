#!/bin/bash
set -euo pipefail

mkdir -p nextclade

NUMBER_WORKERS=32
i=1
for FILE in splits/*.fasta; do
    # This line ensures that at most NUMBER_WORKERS processes are run in parallel. The jobs are run in
    # NUMBER_WORKERS-sized batches, this means that not always NUMBER_WORKERS processes will be running.
    if ((i==0)) ; then
      wait
    fi
    i=$(((i+1)%NUMBER_WORKERS))

    FN=$(basename -- "$FILE")
    FN="${FN%.fasta}"
    nextclade \
        --jobs 12 \
        --input-fasta $FILE \
        --output-csv nextclade/${FN}.csv &
done
wait
