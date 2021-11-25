#!/bin/bash
set -euo pipefail

while getopts i:t:f:m:r:x:n: flag
do
    case "${flag}" in
        r) REFERENCE_PATH=${OPTARG};;
    esac
done

echo "Create directories"
mkdir -p alignments_with_ref
mkdir -p alignments

echo "Aligning each split file"

NUMBER_WORKERS=32
i=1
for FILE in splits/*.fasta; do
    # This line ensures that at most NUMBER_WORKERS processes are run in parallel. The jobs are run in
    # NUMBER_WORKERS-sized batches, this means that not always NUMBER_WORKERS processes will be running.
    if ((i==0)) ; then
      wait
    fi
    i=$(((i+1)%NUMBER_WORKERS))

    echo "Run worker $i"
    FN=$(basename -- "$FILE")
    FN="${FN%.fasta}"
    mafft \
        --addfragments $FILE \
        --keeplength \
        --auto \
        --thread 1 \
        ${REFERENCE_PATH} > alignments_with_ref/${FN}.fasta &
done
wait

echo "Removing reference from each split alignment"
for FILE in alignments_with_ref/*; do
    FN=$(basename -- "$FILE")
    FN="${FN%.fasta}"
    awk 'BEGIN { RS = ">";FS = "\n" } {if (NR>2) {print ">"$0}}' $FILE > alignments/${FN}.fasta
    # There is a blank first record, second record is the reference, all records thereafter are sequences we want to keep
done

echo "Cleaning up directories"
rm -rf alignments_with_ref
