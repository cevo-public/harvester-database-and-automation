#!/bin/bash
set -euo pipefail

echo "username=${NETHZ_USERNAME}" > /credentials.txt
echo "password=${NETHZ_PASSWORD}" >> /credentials.txt
echo "domain=d.ethz.ch" >> /credentials.txt

echo NAS path: $NAS_PATH

DIR=pangolin
URL=$NAS_PATH/pangolin

DIR1=backup
URL1=$NAS_PATH/backup

# Mount pangolin folder
timestamp=`date +%Y/%m/%d-%H:%M:%S`
echo "[$timestamp] Mounting pangolin file server"
mkdir -p "/mnt/$DIR"
mount -v -t cifs "$URL" "/mnt/$DIR" -o credentials=/credentials.txt

echo "[$timestamp] Mounting backup file server"
mkdir -p "/mnt/$DIR1"
mount -v -t cifs "$URL1" "/mnt/$DIR1" -o credentials=/credentials.txt

# Execute program
Rscript R/automation/auto_import_sequences.R
